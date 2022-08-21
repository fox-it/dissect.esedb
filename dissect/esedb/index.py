from __future__ import annotations

import struct
import uuid
from functools import cached_property
from typing import TYPE_CHECKING

from dissect.esedb.c_esedb import CODEPAGE, JET_bitIndex, JET_coltyp, RecordValue
from dissect.esedb.cursor import Cursor
from dissect.esedb.lcmapstring import map_string
from dissect.esedb.page import Node, Page
from dissect.esedb.record import Record

if TYPE_CHECKING:
    from dissect.esedb.table import Column, Table


JET_cbKeyMost_OLD = 255


class Index(object):
    """Represents an index on a table.

    This is still very much WIP but works for basic indexes.
    For example, none of the special flags are currently implemented.

    Args:
        table: The table this index is from.
        record: The record in the catalog for this index.
    """

    def __init__(self, table: Table, record: Record = None):
        self.table = table
        self.record = record
        self.esedb = table.esedb

        self.name = record.get("Name")
        self.flags = JET_bitIndex(record.get("Flags"))
        self._key_most = record.get("KeyMost") or JET_cbKeyMost_OLD
        self._var_seg_mac = record.get("VarSegMac") or self._key_most

    @cached_property
    def root(self) -> Page:
        """Return the root page of this index."""
        return self.esedb.page(self.record.get("ColtypOrPgnoFDP"))

    @cached_property
    def column_ids(self) -> list[int]:
        """Return a list of column IDs that are used in this index."""
        column_ids = []
        key_field_ids = self.record.get("KeyFldIDs")
        if len(key_field_ids) % 4 == 0:
            for i in range(0, len(key_field_ids), 4):
                _, column_identifier = struct.unpack("<HH", key_field_ids[i : i + 4])
                column_ids.append(column_identifier)
        return column_ids

    @cached_property
    def columns(self) -> list[Column]:
        """Return a list of all columns that are used in this index."""
        return list(map(lambda idx: self.table._column_id_map[idx], self.column_ids))

    def search(self, **kwargs) -> Record:
        """Search the index for the requested values.

        Specify the column and value as a keyword argument.
        """
        key = self.make_key(kwargs)
        node = self.search_key(key)
        return Record(self.table, node)

    def search_key(self, key: bytes) -> Node:
        """Search the index for a specific key.

        Args:
            key: The key to search for.
        """
        cursor = Cursor(self.esedb, self.root)
        return cursor.search(key)

    def key_from_record(self, record: Record) -> bytes:
        """Generate a key for this index from a record.

        Args:
            record: The record to generate a key for.
        """
        values = {c.name: record[c.name] for c in self.columns}
        return self.make_key(values)

    def make_key(self, values: dict[str, RecordValue]) -> bytes:
        """Generate a key out of the given values.

        Args:
            values: A map of the column names and values to generate a key for.
        """
        key_buf = []
        key_remaining = self._key_most

        for column in self.columns:
            if column.name not in values:
                break

            key_part = encode_key(self, column, values[column.name], self._var_seg_mac)
            key_buf.append(key_part)
            key_remaining -= len(key_part)

            if key_remaining <= 0:
                break

        key = b"".join(key_buf)
        if key_remaining < 0:
            key = key[: self._key_most]
        return key

    def __repr__(self) -> str:
        return f"<Index name={self.name}>"


bPrefixNull = 0x00
bPrefixZeroLength = 0x40
bPrefixNullHigh = 0xC0
bPrefixData = 0x7F
bSentinel = 0xFF


def encode_key(index: Index, column: Column, value: RecordValue, max_size: int) -> bytes:
    """Encode various values into their normalized index key form.

    Args:
        column: The column of the value to encode.
        value: The value that needs encoding.
        max_size: The maximum key segment size.
    """
    if value is None:
        return bytes([bPrefixNull])

    # All keys with data are prefixed with 0x7f (bPrefixData)
    # There are other prefixes but we don't support those yet
    key = bytearray([bPrefixData])

    if column.type == JET_coltyp.Bit:
        key.append(0xFF if value else 0x00)

    elif column.type == JET_coltyp.UnsignedByte:
        key.append(value)

    elif column.type == JET_coltyp.Short:
        # Signed integers have their MSB bit flipped
        key += struct.pack(">H", (value ^ (1 << 15)) & 0xFFFF)

    elif column.type == JET_coltyp.Long:
        key += struct.pack(">I", (value ^ (1 << 31)) & 0xFFFFFFFF)

    elif column.type in (JET_coltyp.Currency, JET_coltyp.LongLong):
        key += struct.pack(">Q", (value ^ (1 << 63)) & 0xFFFFFFFFFFFFFFFF)

    elif column.type == JET_coltyp.IEEESingle:
        value = struct.unpack("<I", struct.pack("<f", value))[0]

        if value & (1 << 31):
            # If the high bit is set, all bits are flipped
            value = ~value & ((1 << 32) - 1)
        else:
            # Otherwise only the high bit is flipped
            value ^= 1 << 31

        key += struct.pack(">I", value)

    elif column.type in (JET_coltyp.IEEEDouble, JET_coltyp.DateTime):
        if column.type == JET_coltyp.IEEEDouble:
            value = struct.unpack("<Q", struct.pack("<d", value))[0]

        if value & (1 << 63):
            # If the high bit is set, all bits are flipped
            value = ~value & ((1 << 64) - 1)
        else:
            # Otherwise only the high bit is flipped
            value ^= 1 << 63

        key += struct.pack(">Q", value)

    elif column.is_binary:
        if not len(value):
            # Empty values (but non null)
            key = bytearray([bPrefixZeroLength])
        elif column.is_fixed:
            # Fixed size binary values are added as is
            if len(value) + 1 > max_size:
                value = value[:max_size]
            key += value
        else:
            # Otherwise added as chunks of 8
            num_chunks = (len(value) + 7) // 8
            # Each chunk has 1 header byte, and the key has a header byte
            key_size = (num_chunks * 9) + 1

            normalized_all = True
            if key_size > max_size:
                key_size = max_size
                normalized_all = False

            key_remaining = key_size - 1

            value_offset = 0
            value_remaining = len(value)
            while key_remaining >= 9:
                chunk = value[value_offset : value_offset + 8]
                key += chunk

                if value_remaining <= 8:
                    # Last chunk
                    if value_remaining == 8:
                        key.append(0x08 if normalized_all else 0x09)  # cbFLDBinaryChunk or cbFLDBinaryChunkNormalized
                    else:
                        # Pad to 8 bytes
                        key.extend([0] * (8 - len(chunk)))
                        key.append(len(chunk))
                else:
                    key.append(0x09)  # cbFLDBinaryChunkNormalized
                    value_offset += 8
                    value_remaining -= 8

                key_remaining -= 9

            if key_remaining:
                if value_remaining >= key_remaining:
                    key += value[value_offset : value_offset + key_remaining]
                else:
                    key += value[value_offset : value_offset + value_remaining]
                    key.extend([0] * (key_remaining - value_remaining))

    elif column.is_text:
        # Unicode strings == LCMapStringW
        # ASCII strings == uppercase
        if not len(value):
            # Empty values (but non null) are indicated with 0x40
            key = bytearray([bPrefixZeroLength])
        elif column.encoding in (CODEPAGE.ASCII, CODEPAGE.WESTERN):
            if len(value) + 1 > max_size:
                value = value[:max_size]
            key += value.upper().encode()
            key.append(0)
        else:
            flags = index.record.get("LCMapFlags")
            locale = index.record.get("LocaleName").decode("utf-16-le")
            segment = map_string(value, flags, locale)
            key += segment[:max_size]

    elif column.type == JET_coltyp.UnsignedLong:
        # Unsigned variants are added as is
        key += struct.pack(">I", value)

    elif column.type == JET_coltyp.GUID:
        if isinstance(value, str):
            value = uuid.UUID(value)
        guid_bytes = value.bytes_le
        key += guid_bytes[-6:]
        key += guid_bytes[-8:-6]
        key += guid_bytes[-10:-8]
        key += guid_bytes[-12:-10]
        key += guid_bytes[:-12]

    elif column.type == JET_coltyp.UnsignedShort:
        key += struct.pack(">H", value)

    return bytes(key)
