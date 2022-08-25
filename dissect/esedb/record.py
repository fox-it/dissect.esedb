from __future__ import annotations

import functools
import struct
from binascii import hexlify
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Optional

from dissect.util.xmemoryview import xmemoryview

from dissect.esedb import compression
from dissect.esedb.c_esedb import PAGE_FLAG, TAGFLD_HEADER, RecordValue, c_esedb

if TYPE_CHECKING:
    from dissect.esedb.page import Node
    from dissect.esedb.table import Column, Table


def noop(value: Any):
    return value


class Record:
    """Wrapper class for records in a table.

    The actual parsing of the data is done in :class:`RecordData`, but this class allows you to easily
    retrieve all the values by either using the `.get()` method, accessing them as attributes or dictionary keys
    on this class.

    Args:
        table: The table this record is from.
        node: The node of this record.
    """

    def __init__(self, table: Table, node: Node):
        self._table = table
        self._esedb = table.esedb
        self._node = node
        self._data = RecordData(table, node)

    def get(self, attr: str, raw: bool = False) -> RecordValue:
        """Retrieve a value from the record with the given name.

        Optionally receive the raw data as it's stored in the record.

        Args:
            attr: The column name to retrieve the value of.
            raw: Whether to return the raw data stored in the record instead of the parsed value.
        """
        column = self._table.column(attr)
        return self._data.get(column, raw)

    def __getitem__(self, attr: str) -> RecordValue:
        return self.get(attr)

    def __getattr__(self, attr: str) -> RecordValue:
        try:
            return self.get(attr)
        except KeyError:
            return object.__getattribute__(self, attr)

    def __str__(self) -> str:
        column_values = serialise_record_column_values(self, max_columns=None)
        return f"<Record {column_values}>"

    def __repr__(self) -> str:
        column_values = serialise_record_column_values(self)
        return f"<Record {column_values}>"


class RecordData:
    """Record class for parsing and interacting with the on-disk record format.

    Templated columns are currently not implemented.

    Args:
        table: The table this record is from.
        data: The node data of this record.

    Raises:
        NotImplementedError: If old format tagged fields are encountered.
    """

    def __init__(self, table: Table, node: Node):
        self.table = table
        self.esedb = table.esedb
        self.node = node
        self.data = node.data

        self.header = None
        self._values = {}

        self._last_fixed_id = None
        self._last_variable_id = None

        self._fixed_null_bitmap = None

        self._variable_offset_start = None
        self._variable_data_start = None
        self._variable_offsets = []

        self._tagged_data_start = None
        self._tagged_data_count = 0
        self._tagged_data_view = None
        self._tagged_fields = {}

        if len(self.data) >= 4:
            self.header = c_esedb.RECHDR(self.data)
            self._last_fixed_id = self.header.fidFixedLastInRec
            self._last_variable_id = self.header.fidVarLastInRec
            self._variable_offset_start = self.header.ibEndOfFixedData

            # There's a bitmap between the end of the fixed data and the start of variable data that indicates
            # if a fixed column is null.
            bitmap_start = self._variable_offset_start - ((self._last_fixed_id + 7) // 8)
            self._fixed_null_bitmap = self.data[bitmap_start : self._variable_offset_start]

            # Calculate where the variable offsets array and data start
            # This info is needed for parsing both variable and tagged data
            # Variable identifiers start from 128
            num_variable = self._last_variable_id - 127
            # Variable offset end == variable data start
            self._variable_data_start = self._variable_offset_start + (num_variable * 2)

            if num_variable > 0 and len(self.data) >= 4 + (num_variable * 2):
                # Parse the variable offsets already, if we have them
                # There can only be 128 at most, so this shouldn't be an expensive operation
                self._variable_offsets = struct.unpack(
                    "<%dH" % num_variable, self.data[self._variable_offset_start : self._variable_data_start]
                )

            self._tagged_data_start = self._variable_data_start
            if self._variable_offsets:
                self._tagged_data_start += self._variable_offsets[-1] & 0x7FFF

            if len(self.data) >= self._tagged_data_start + 4:
                if node.tag.page.flags & PAGE_FLAG.NewRecordFormat == 0:
                    raise NotImplementedError("Record has tagged fields in an old format, which is not implemented yet")

                tag_value = int.from_bytes(self.data[self._tagged_data_start : self._tagged_data_start + 4], "little")
                first_tagged_field = TagField(self, tag_value)

                tagged_field_data_start = self._tagged_data_start
                tagged_field_data_end = tagged_field_data_start + first_tagged_field.offset
                tagged_field_data = self.data[self._tagged_data_start : tagged_field_data_end]

                self._tagged_data_count = first_tagged_field.offset // 4  # sizeof(TAGFLD)
                self._tagged_data_view = xmemoryview(tagged_field_data, "<I")
                self._tagged_fields[first_tagged_field.identifier] = first_tagged_field

    def get(self, column: Column, raw: bool = False) -> RecordValue:
        """Retrieve the value for the specified column.

        Optionally receive the raw data as it's stored in the record.

        If the database has been opened in impacket compatibility mode, skip most of the parsing and return the values
        that impacket expects.

        Args:
            column: The column to retrieve the value of.
            raw: Whether to return the raw data stored in the record instead of the parsed value.
        """
        value = None
        tag_field = None

        if not self.header:
            return value

        if column.is_fixed:
            value = self._get_fixed(column)

        elif column.is_variable:
            value = self._get_variable(column)

        elif column.is_tagged:
            tag_field, value = self._get_tagged(column)

        if raw:
            if isinstance(value, memoryview):
                value = value.tobytes()
            return value

        if value is not None:
            return self._parse_value(column, value, tag_field)

    def _parse_value(self, column: Column, value: bytes, tag_field: TagField = None) -> RecordValue:
        """Parse the raw value into the appropriate type.

        For tagged columns, also interpret things like multi-values, separated and compressed data.
        """
        ctype = column.ctype
        parse_func = ctype.parse
        if column.is_text:
            parse_func = functools.partial(ctype.parse, encoding=column.encoding)

        if self.esedb.impacket_compat:
            if tag_field and tag_field.flags & TAGFLD_HEADER.Compressed:
                value = None
            elif tag_field and tag_field.flags & TAGFLD_HEADER.MultiValues:
                value = hexlify(value)
            elif parse_func != bytes:
                value = parse_func(value)
            else:
                value = hexlify(value)
        else:
            if tag_field:
                if tag_field.flags & TAGFLD_HEADER.MultiValues:
                    value = self._parse_multivalue(value, tag_field)
                else:
                    if tag_field.flags & TAGFLD_HEADER.Separated:
                        value = self.table.get_long_value(bytes(value))
                    elif tag_field.flags & TAGFLD_HEADER.Compressed:
                        # Long values are already decompressed during retrieval
                        value = compression.decompress(value)

            parse_func = parse_func or noop
            if tag_field and tag_field.flags & TAGFLD_HEADER.MultiValues:
                value = list(map(parse_func, value))
            else:
                value = parse_func(value)

        return value

    def _parse_multivalue(self, value: bytes, tag_field: TagField):
        fSeparatedInstance = 0x8000

        if tag_field.flags & TAGFLD_HEADER.TwoValues:
            # Optimized storage for when a multi-value only has two values
            # First byte is the size of the first value, calculate the size of the second value from that
            first_size = value[0]
            second_size = len(value) - (1 + first_size)
            value = [value[1 : 1 + first_size], value[1 + first_size : 1 + first_size + second_size]]
        elif tag_field.flags & TAGFLD_HEADER.MultiValues:
            # Regular multi-value storage, starts with an array of USHORT offsets to the actual values
            # Just calculate the amount of values from the first entry
            # Individual offsets can have a fSeparatedInstance (0x8000) flag set
            first_value_offset = struct.unpack("<H", value[0:2])[0] & 0x7FFF
            num_values = first_value_offset // 2  # sizeof(USHORT)
            value_offsets = struct.unpack(f"<{num_values}H", value[:first_value_offset]) + (len(value),)

            values = []
            for i in range(num_values):
                offset = value_offsets[i]
                data = value[offset & 0x7FFF : value_offsets[i + 1] & 0x7FFF]
                if offset & fSeparatedInstance:
                    data = self.table.get_long_value(bytes(data))
                values.append(data)
            value = values

        if tag_field.flags & TAGFLD_HEADER.Compressed:
            # Only the first entry appears to be compressed
            value[0] = compression.decompress(value[0])

        return value

    def _get_fixed(self, column: Column) -> Optional[bytes]:
        """Parse a specific fixed column."""
        if column.identifier <= self._last_fixed_id:
            # Check if it's not null
            bit_idx_identifier = column.identifier - 1
            bitmap_offset, bitmap_shift = divmod(bit_idx_identifier, 8)
            if self._fixed_null_bitmap[bitmap_offset] & (1 << bitmap_shift):
                return None

            # Fixed data starts right after the header, which is 4 bytes
            offset = 4 + column.offset
            value = self.data[offset : offset + column.size]
        else:
            # If the column has a default, use that
            # If not, this defaults to None
            value = column.default

        return value

    def _get_variable(self, column: Column) -> Optional[bytes]:
        """Parse a specific variable column."""
        if column.identifier <= self._last_variable_id:
            identifier_idx = column.identifier - 128
            if identifier_idx == 0:
                value_start = 0
            else:
                # Start of this value is the end of the previous value
                # Even empty values have the offset encoded in them
                value_start = self._variable_offsets[identifier_idx - 1] & 0x7FFF

            # The value at the own index is the end offset of this value
            value_end = self._variable_offsets[identifier_idx]

            # If the MSB has been set, it means the entry is empty
            if value_end & 0x8000 == 0:
                # Offset everything with the variable data value starting offset
                value_offset = self._variable_data_start
                value = self.data[value_offset + value_start : value_offset + value_end]
            else:
                value = None
        else:
            # If the column has a default, use that
            # If not, this defaults to None
            value = column.default

        return value

    def _get_tagged(self, column: Column) -> Optional[bytes]:
        """Parse a specific tagged column."""
        tag_field = None

        idx = self._find_tag_field_idx(column.identifier)
        if idx is not None:
            tag_field = self._get_tag_field(idx)

            data_start = tag_field.offset
            if tag_field.has_extended_info:
                data_start += 1

            if idx + 1 < self._tagged_data_count:
                data_end = self._get_tag_field(idx + 1).offset
            else:
                data_end = len(self.data)

            if not tag_field.is_null:
                offset = self._tagged_data_start
                value = self.data[offset + data_start : offset + data_end]
        else:
            # If the column has a default, use that
            # If not, this defaults to None
            value = column.default

        return tag_field, value

    @lru_cache(4096)
    def _get_tag_field(self, idx: int) -> TagField:
        """Retrieve the :class:`TagField` at the given index in the ``TAGFLD`` array."""
        return TagField(self, self._tagged_data_view[idx])

    @lru_cache(4096)
    def _find_tag_field_idx(self, identifier: int, is_derived: bool = False) -> Optional[TagField]:
        """Find a tag field by identifier and optional derived flag.

        Performs a binary search in the tagged field array for the given identifier. The comparison algorithm used is
        derived from the ``TAGFLD::CmpTagfld2`` function in the Microsoft codebase.

        Args:
            identifier: The column identifier to look for.
            is_derived: A flag indicating if the column is derived.

        Returns:
            A :class:`TagField` object for the found tag field, or ``None`` if it's not found.
        """
        if self._tagged_data_count == 0:
            return None

        lookup = identifier
        if is_derived:
            lookup |= TagField.fDerived << 16  # fDerived

        # TAGFLD::CmpTagfld2
        flip_derived = TagField.fDerived << 16
        mask = flip_derived | ((1 << 16) - 1)
        value2 = flip_derived ^ (lookup & mask)

        min_idx = 0
        max_idx = self._tagged_data_count - 1

        while min_idx != max_idx:
            test_idx = min_idx + (max_idx - min_idx) // 2

            tag_value = self._tagged_data_view[test_idx]
            value1 = flip_derived ^ (tag_value & mask)
            if value1 < value2:
                min_idx = test_idx + 1
            elif value1 == value2:
                min_idx = test_idx
                break
            else:
                max_idx = test_idx

        tag_value = self._tagged_data_view[min_idx]
        if tag_value & 0xFFFF == identifier:
            return min_idx

        return None


class TagField:
    """Represents a ``TAGFLD``, which contains information about a tagged field in a record."""

    __slots__ = ("record", "identifier", "_offset", "offset", "has_extended_info", "flags")

    fNullSmallPage = 0x2000
    fDerived = 0x8000

    def __init__(self, record: RecordData, value: int):
        self.record = record
        self.identifier = value & 0xFFFF
        self._offset = (value >> 16) & 0xFFFF

        if record.esedb.has_small_pages:
            self.offset = self._offset & 0x1FFF  # & maskIb
            self.has_extended_info = bool(self._offset & 0x4000)  # & fExtendedInfo
        else:
            self.offset = self._offset & 0x7FFF  # & maskIb
            self.has_extended_info = True

        if self.has_extended_info and len(self.record.data) >= self.record._tagged_data_start + self.offset:
            self.flags = TAGFLD_HEADER(self.record.data[self.record._tagged_data_start + self.offset])
        else:
            self.flags = TAGFLD_HEADER.Invalid  # Made up flag member to keep the types consistent

    def __repr__(self) -> str:
        return f"<TagField identifier={self.identifier} offset={self.offset:#x} flags={str(self.flags).split('.')[1]}>"

    @property
    def is_null(self) -> bool:
        """Return whether this tagged field is null."""
        if self.record.esedb.has_small_pages:
            return bool(self._offset & TagField.fNullSmallPage)
        else:
            return bool(self.flags & TAGFLD_HEADER.Null)

    @property
    def is_derived(self) -> bool:
        """Return whether this tagged field is derived."""
        return bool(self._offset & TagField.fDerived)


def serialise_record_column_values(record: Record, column_names: list[str] = None, max_columns: int = 10) -> str:
    column_names = column_names or record._table.column_names
    columns_with_values = []
    for name in column_names:
        try:
            value = record.get(name)
        except Exception:
            value = "!ERROR!"

        if value is not None:
            columns_with_values.append((name, value))

        if max_columns and len(columns_with_values) >= max_columns:
            break

    reprs = " ".join([f"{name}={value!r}" for (name, value) in columns_with_values])
    has_more = " ..." if max_columns and len(column_names) > max_columns else ""
    return f"{reprs}{has_more}"
