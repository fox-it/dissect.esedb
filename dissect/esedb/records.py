import functools
import struct
import uuid
from binascii import hexlify
from typing import Any, List

from dissect.esedb.exceptions import CompressedTaggedDataError, InvalidColumn
from dissect.esedb.utils import (
    CODEPAGE_ASCII,
    COLUMN_TYPE,
    COLUMN_TYPE_MAP,
    DATA_DEFINITION_HEADER_SIZE,
    TAGGED_DATA_FORMAT_INDEX,
    TAGGED_DATA_FORMAT_LINEAR,
    TAGGED_DATA_TYPE,
    c_esedb,
    noop,
)


class Record:
    def __init__(self, table, buf):
        self.esedb = table.esedb
        self.table = table

        self._buf = buf
        self._data_definition = DataDefinition(self, buf)

    def get(self, attr) -> Any:
        column = self.table.get_column(attr)
        if not column:
            raise InvalidColumn(f"No column with this name in the table {self.table.name!r}: {attr!r}")
        value = self._data_definition.get(column)

        if isinstance(value, str):
            value = value.strip("\x00")

        if column.is_uuid():
            value = uuid.UUID(bytes_le=value.value)
            # return serialised UUID and not an UUID object
            value = str(value)

        return value

    def __repr__(self):
        column_values = serialise_record_column_values(self)
        return f"<Record {column_values}>"


class TaggedDataInfo:
    __slots__ = ("offset", "size", "flags", "has_data_flags")

    def __init__(self, offset=None, flags=None, has_data_flags=False):
        self.offset = offset
        self.size = None
        self.flags = flags
        self.has_data_flags = has_data_flags

    def __repr__(self):
        return "<TaggedDataInfo offset=0x{:x} size=0x{:x} flags=0x{:x} has_data_flags={!r}>".format(
            self.offset, self.size, self.flags, self.has_data_flags
        )


class DataDefinition:
    def __init__(self, record, data):
        self.record = record
        self.table = record.table
        self.esedb = record.esedb
        self.data = data

        self.header = None
        self._values = {}

        self._tagged_data_mask = 0x3FFF
        self._tagged_data_format = TAGGED_DATA_FORMAT_INDEX
        self._tagged_data_has_flags = False

        esedb = self.esedb
        if esedb.header.format_revision >= 0x11 and esedb.header.page_size > 0x4000:
            self._tagged_data_mask = 0x7FFF

        if esedb.header.format_version == 0x620 and esedb.header.format_revision <= 2:
            self._tagged_data_format = TAGGED_DATA_FORMAT_LINEAR

        if (
            esedb.header.format_version == 0x620
            and esedb.header.format_revision >= 0x11
            and esedb.header.page_size > 0x2000
        ):
            self._tagged_data_has_flags = True

        self._last_fixed_id = None
        self._last_variable_id = None
        self._variable_sizes = []
        self._variable_data_offset = None
        self._variable_value_offset = None
        self._tagged_data_info = {}

        if len(data) >= 4:
            self.header = c_esedb.data_definition_header(data)
            self._last_fixed_id = self.header.last_fixed_id
            self._last_variable_id = self.header.last_variable_id
            self._variable_data_offset = self.header.variable_data_offset

            # Calculate where the variable sizes array and data start

            # Parse the variable sizes already, if we have them
            num_variable = self._last_variable_id - 127
            self._variable_value_offset = self._variable_data_offset + (num_variable * 2)

            if num_variable > 0:
                self._variable_sizes = struct.unpack(
                    "<%dH" % num_variable, self.data[self._variable_data_offset : self._variable_value_offset]
                )

        self._parsed_fixed = False
        self._parsed_variable = False
        self._parsed_tagged = False

    def get(self, column):
        if not self.header:
            return None

        identifier = column.identifier
        if identifier <= self._last_fixed_id and not self._parsed_fixed:
            self._parse_fixed()
        elif 127 < identifier <= self._last_variable_id and not self._parsed_variable:
            self._parse_variable()
        elif identifier > 255:
            if not self._parsed_tagged:
                self._parse_tagged_data_info()

            if identifier in self._tagged_data_info and identifier not in self._values:
                self._parse_tagged(column)

        return self._values.get(identifier, None)

    def _parse_fixed(self):
        if self._parsed_fixed or not self.header:
            return
        self._parsed_fixed = True

        buf = self.data
        # Fixed data starts right after the header
        offset = DATA_DEFINITION_HEADER_SIZE
        values = self._values

        last_fixed_id = self._last_fixed_id

        for column in self.table.columns:
            identifier = column.identifier
            column_type = column.type.value

            if identifier > last_fixed_id:
                break

            # Just read fixed size columns using known sizes
            ctype = COLUMN_TYPE_MAP[column_type]
            if column.record:
                size = column.record.get("SpaceUsage")
            else:
                size = len(ctype.parse)

            value = buf[offset : offset + size]

            if ctype.parse:
                value = ctype.parse(value)

            values[identifier] = value
            offset += size

    def _parse_variable(self):
        if self._parsed_variable or not self.header:
            return
        self._parsed_variable = True

        buf = self.data
        offset = self._variable_value_offset
        values = self._values

        # Fixed size identifiers count from 1 to 127
        # Variable size identifiers start from 128
        last_variable_id = self._last_variable_id
        variable_sizes = self._variable_sizes

        for column in self.table.columns:
            column_type = column.type.value
            identifier = column.identifier

            if identifier < 128:
                continue

            if identifier > last_variable_id:
                break

            size = variable_sizes[identifier - 128]
            if size & 0x8000 == 0:
                ctype = COLUMN_TYPE_MAP[column_type]
                v = buf[offset : offset + size]
                if column_type in (COLUMN_TYPE.JET_coltypText, COLUMN_TYPE.JET_coltypLongText):
                    v = ctype.parse(v, column.record.get("PagesOrLocale") if column.record else CODEPAGE_ASCII)
                elif ctype.parse:
                    v = ctype.parse(v)

                values[identifier] = v
                offset += size

    def _parse_tagged_data_info(self):
        if self._parsed_tagged or not self.header:
            return
        self._parsed_tagged = True

        buf = self.data
        tagged_data_info = self._tagged_data_info
        # Tagged data info array starts after the variable data
        tagged_data_offset = self._variable_value_offset + sum(self._variable_sizes)
        if tagged_data_offset + 4 >= len(buf):
            return

        if self._tagged_data_format == TAGGED_DATA_FORMAT_LINEAR:
            raise NotImplementedError("tagged data is in linear format")

        offset = tagged_data_offset
        # We don't know the amount of tagged data items, so start with this boundary
        # After having parsed the first entry, we update the boundary
        tag_index_end = len(buf)
        prev_info = None
        has_flags = self._tagged_data_has_flags

        while offset < tag_index_end:
            identifier, value_offset = struct.unpack("<2H", buf[offset : offset + 4])
            value_offset, flags = value_offset & 0x3FFF, value_offset & ~0x3FFF

            # Just store the absolute offset directly, no point in storing the relative offset
            tag_info = TaggedDataInfo(tagged_data_offset + value_offset, flags, has_flags or flags & 0x4000 != 0)
            tagged_data_info[identifier] = tag_info

            if not prev_info:
                # This is the first iteration, update boundary
                # First tagged data starts here, so info array ends there too
                tag_index_end = tag_info.offset

            if prev_info:
                prev_info.size = tag_info.offset - prev_info.offset

            prev_info = tag_info
            offset += 4

        prev_info.size = len(buf) - prev_info.offset

    def _parse_tagged(self, column):
        buf = self.data
        column_type = column.type.value
        identifier = column.identifier
        if identifier < 256 or identifier not in self._tagged_data_info:
            return

        tag_info = self._tagged_data_info[identifier]
        size = tag_info.size
        offset = tag_info.offset
        data_flags = 0
        if tag_info.has_data_flags:
            data_flags = TAGGED_DATA_TYPE(ord(buf[offset : offset + 1]))
            size -= 1
            offset += 1

            if data_flags & TAGGED_DATA_TYPE.COMPRESSED:
                raise CompressedTaggedDataError("compressed")

        ctype = COLUMN_TYPE_MAP[column_type]
        parse_func = ctype.parse

        v = buf[offset : offset + size]

        if column_type in (COLUMN_TYPE.JET_coltypText, COLUMN_TYPE.JET_coltypLongText):
            encoding = column.record.get("PagesOrLocale") if column.record else CODEPAGE_ASCII
            parse_func = functools.partial(ctype.parse, encoding=encoding)

        if self.esedb.impacket_compat:
            if data_flags & TAGGED_DATA_TYPE.COMPRESSED:
                v = None
            elif data_flags & TAGGED_DATA_TYPE.MULTI_VALUE:
                v = hexlify(v)
            elif parse_func:
                v = parse_func(v)
            else:
                v = hexlify(v)
        else:
            parse_func = parse_func or noop

            if data_flags & TAGGED_DATA_TYPE.STORED:
                v = self.table.get_long_value(v)

            if data_flags & TAGGED_DATA_TYPE.MULTI_VALUE_SIZE_DEF:
                # Data starts with an uint8 size definition
                # Presumably this is the size of all the values in the multi value
                # Just parse as many values as we can with this size
                (value_size,) = struct.unpack("<B", v[:1])
                num_values = (size - 1) // value_size

                v = v[1:]
                v = [parse_func(v[i * value_size : (i * value_size) + value_size]) for i in range(num_values)]
            elif data_flags & TAGGED_DATA_TYPE.MULTI_VALUE:
                # Data starts with an array of uint16 offsets to the actual values
                # We don't know the amount of offsets, just like tagged data info
                (first_value_offset,) = struct.unpack("<H", v[0:2])
                num_values = first_value_offset // 2
                value_offsets = struct.unpack("<{}H".format(num_values), v[:first_value_offset]) + (size,)

                v = [parse_func(v[value_offsets[i] : value_offsets[i + 1]]) for i in range(num_values)]
            else:
                v = parse_func(v)

        self._values[identifier] = v


def serialise_record_column_values(record: Record, column_names: List[str] = None, columns_cap=10) -> str:
    column_names = column_names or record.table.column_names
    columns_with_values = []
    for name in column_names:
        value = record.get(name)
        if value:
            columns_with_values.append((name, value))
        if len(columns_with_values) >= columns_cap:
            break
    column_reprs = " ".join([f"{name}={value!r}" for (name, value) in columns_with_values])
    has_more = " ..." if len(column_names) > columns_cap else ""
    return f"{column_reprs}{has_more}"
