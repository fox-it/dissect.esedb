from __future__ import annotations

import struct
from functools import cached_property
from typing import TYPE_CHECKING, Any

from dissect.esedb import compression
from dissect.esedb.btree import BTree
from dissect.esedb.c_esedb import (
    CODEPAGE,
    COLUMN_TYPE_MAP,
    SYSOBJ,
    ColumnType,
    JET_coltyp,
    RecordValue,
)
from dissect.esedb.exceptions import NoNeighbourPageError
from dissect.esedb.index import Index
from dissect.esedb.record import Record

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dissect.esedb.cursor import Cursor
    from dissect.esedb.esedb import EseDB
    from dissect.esedb.page import Page


class Table:
    """Represents a table in an ESE database.

    Contains all the relevant metadata of the table, as well as all columns and indices that have been added
    by the catalog.

    Args:
        esedb: An instance of :class:`~dissect.esedb.esedb.EseDB`.
        name: The table name.
        root_page: The root page of the table.
        columns: A list of :class:`Column` for this table.
        indexes: A list of :class:`~dissect.esedb.index.Index` for this table.
        record: The :class:`~dissect.esedb.record.Record` of this table from the catalog table.
    """

    def __init__(
        self,
        esedb: EseDB,
        name: str,
        root_page: int,
        columns: list[Column] | None = None,
        indexes: list[Index] | None = None,
        record: Record = None,
    ):
        self.esedb = esedb

        self._column_name_map: dict[str, Column] = {}
        self._column_id_map: dict[int, Column] = {}
        self._index_name_map: dict[str, Index] = {}

        self.name = name
        self.root_page = root_page
        self.columns: list[Column] = []
        self.indexes: list[Index] = []

        # Set by the catalog during parsing
        self._long_value_record: Record = None
        self._long_callback_record: Record = None

        self._fixed_value_offset = 0

        columns = columns or []
        for column in columns:
            self._add_column(column)

        indexes = indexes or []
        for index in indexes:
            self._add_index(index)

        self.record = record

    def __repr__(self) -> str:
        return f"<Table name={self.name!r}>"

    @cached_property
    def root(self) -> Page:
        """Return the root page of the table."""
        return self.esedb.page(self.root_page)

    @cached_property
    def lv_page(self) -> Page:
        """Return the long value page of the table.

        Raises:
            TypeError: If the table has no long values.
        """
        if not self._long_value_record:
            raise TypeError(f"Table has no long values: {self.name}")

        return self.esedb.page(self._long_value_record.get("ColtypOrPgnoFDP"))

    def column(self, name: str) -> Column:
        """Return the column with the given name.

        Args:
            name: The name of the column to return.

        Raises:
            KeyError: If no column with the given name exists.
        """
        try:
            return self._column_name_map[name]
        except KeyError:
            raise KeyError(f"No column with this name in table {self.name}: {name}")

    @property
    def column_names(self) -> list[str]:
        """Return a list of all the column names."""
        return list(self._column_name_map.keys())

    @property
    def primary_index(self) -> Index | None:
        # It's generally the first index, but loop just in case
        for index in self.indexes:
            if index.is_primary:
                return index
        return None

    def cursor(self) -> Cursor | None:
        """Create a new cursor for this table."""
        primary_idx = self.primary_index
        if primary_idx:
            return primary_idx.cursor()
        return None

    def index(self, name: str) -> Index:
        """Return the index with the given ``name``.

        Args:
            name: The name of the index to return.

        Raises:
            KeyError: If no index with the given name exists.
        """
        try:
            return self._index_name_map[name]
        except KeyError:
            raise KeyError(f"No index with this name in table {self.name}: {name}")

    def find_index(self, column_names: list[str]) -> Index | None:
        """Find the most suitable index to search for the given columns.

        Args:
            column_names: A list of column names to find the best index for.
        """
        best_match = 0
        best_index = None
        for index in self.indexes:
            # We want to find the index that has the most matching columns in the order they are indexed
            i = 0
            for column in index.columns:
                if column.name not in column_names:
                    break
                i += 1

            if i > best_match:
                best_index = index
                best_match = i

        return best_index

    def search(self, **kwargs: RecordValue) -> Record | None:
        """Search for a record in the table.

        Args:
            **kwargs: The columns and values to search for.

        Returns:
            The first record that matches the search criteria, or ``None`` if no record was found.
        """
        return self.cursor().search(**kwargs)

    def records(self) -> Iterator[Record]:
        """Return an iterator of all the records of the table."""
        for node in self.root.iter_leaf_nodes():
            yield Record(self, node)

    def get_long_value(self, key: bytes) -> bytes:
        """Retrieve a value from the long value page of the table.

        Args:
            key: The lookup key for the long value.
        """
        rkey = key[::-1]
        btree = BTree(self.esedb, self.lv_page)
        header = btree.search(rkey)

        _, size = struct.unpack("<2I", header.data)
        chunks = []
        chunk_offsets = []

        while True:
            try:
                node = btree.next()
                if not node.key.startswith(rkey):
                    break
            except NoNeighbourPageError:
                break

            chunks.append(node.data)

            chunk_offset = struct.unpack(">I", node.key[-4:])[0]
            chunk_offsets.append(chunk_offset)

        chunk_offsets.append(size)

        buf = []
        chunk_offset = 0
        for chunk, next_chunk_offset in zip(chunks, chunk_offsets[1:]):
            # Chunk sizes should be used to determine if a chunk is compressed
            if len(chunk) != next_chunk_offset - chunk_offset:
                chunk = compression.decompress(chunk)
            buf.append(chunk)
            chunk_offset += next_chunk_offset

        return b"".join(buf)

    def _add_column(self, column: Column) -> None:
        """Add a column to the table."""
        self.columns.append(column)
        self._column_name_map[column.name] = column
        self._column_id_map[column.identifier] = column

        # Precalculate fixed value offsets
        if column.identifier < 128:
            column._offset = self._fixed_value_offset
            self._fixed_value_offset += column.size

    def _add_index(self, index: Index) -> None:
        """Add an index to the table."""
        self.indexes.append(index)
        self._index_name_map[index.name] = index


class Column:
    def __init__(self, identifier: int, name: str, type_: JET_coltyp, record: Record | None = None):
        self.identifier = identifier
        self.name = name
        self.type = type_

        # Set by the table when added, only relevant for fixed value columns
        self._offset = None

        self.record = record

    def __repr__(self) -> str:
        return f"<Column name={self.name!r} identifier={self.identifier:#x} type={self.type} size={self.size}>"

    @property
    def offset(self) -> int:
        return self._offset

    @cached_property
    def is_fixed(self) -> bool:
        return self.identifier <= 127

    @cached_property
    def is_variable(self) -> bool:
        return 127 < self.identifier <= 255

    @cached_property
    def is_tagged(self) -> bool:
        return self.identifier > 255

    @cached_property
    def is_text(self) -> bool:
        return self.type in (JET_coltyp.Text, JET_coltyp.LongText)

    @cached_property
    def is_binary(self) -> bool:
        return self.type in (JET_coltyp.Binary, JET_coltyp.LongBinary)

    @cached_property
    def size(self) -> int:
        if self.record and self.record.get("SpaceUsage"):
            return self.record.get("SpaceUsage")
        return self.ctype.size

    @cached_property
    def default(self) -> Any | None:
        if self.record and self.record.get("DefaultValue"):
            return self.record.get("DefaultValue")
        return None

    @cached_property
    def encoding(self) -> CODEPAGE | None:
        if self.is_text:
            return CODEPAGE(self.record.get("PagesOrLocale")) if self.record else CODEPAGE.ASCII
        return None

    @cached_property
    def ctype(self) -> ColumnType:
        return COLUMN_TYPE_MAP[self.type.value]


class Catalog:
    """Parse and interact with the catalog table.

    The catalog is a special table that contains the metadata for all the other tables in the database.

    Args:
        esedb: An instance of :class:`~dissect.esedb.esedb.EseDB`.
        root_page: The root page of the catalog table.
    """

    CATALOG_COLUMNS = (
        Column(1, "ObjidTable", JET_coltyp.Long),
        Column(2, "Type", JET_coltyp.Short),
        Column(3, "Id", JET_coltyp.Long),
        Column(4, "ColtypOrPgnoFDP", JET_coltyp.Long),
        Column(5, "SpaceUsage", JET_coltyp.Long),
        Column(6, "Flags", JET_coltyp.Long),
        Column(7, "PagesOrLocale", JET_coltyp.Long),
        Column(8, "RootFlag", JET_coltyp.Bit),
        Column(9, "RecordOffset", JET_coltyp.Short),
        Column(10, "LCMapFlags", JET_coltyp.Long),
        Column(11, "KeyMost", JET_coltyp.UnsignedShort),
        Column(12, "LVChunkMax", JET_coltyp.Long),
        Column(128, "Name", JET_coltyp.Text),
        Column(129, "Stats", JET_coltyp.Binary),
        Column(130, "TemplateTable", JET_coltyp.Text),
        Column(131, "DefaultValue", JET_coltyp.Binary),
        Column(132, "KeyFldIDs", JET_coltyp.Binary),
        Column(133, "VarSegMac", JET_coltyp.Binary),
        Column(134, "ConditionalColumns", JET_coltyp.Binary),
        Column(135, "TupleLimits", JET_coltyp.Binary),
        Column(136, "Version", JET_coltyp.Binary),
        Column(137, "SortID", JET_coltyp.Binary),
        Column(256, "CallbackData", JET_coltyp.LongBinary),
        Column(257, "CallbackDependencies", JET_coltyp.LongBinary),
        Column(258, "SeparateLV", JET_coltyp.LongBinary),
        Column(259, "SpaceHints", JET_coltyp.LongBinary),
        Column(260, "SpaceDeferredLVHints", JET_coltyp.LongBinary),
        Column(261, "LocaleName", JET_coltyp.LongBinary),
    )

    def __init__(self, esedb: EseDB, root_page: Page):
        self.esedb = esedb
        self.root_page = root_page
        self.tables = []
        self._table_name_map = {}

        # Create a dummy table with a preset list of columns
        self._ctable = Table(esedb, None, root_page, columns=self.CATALOG_COLUMNS)

        cur_table = None
        for rec in self._ctable.records():
            rtype = rec.get("Type")
            if rtype == SYSOBJ.Table:
                cur_table = Table(self.esedb, rec.get("Name"), rec.get("ColtypOrPgnoFDP"), record=rec)
                self.tables.append(cur_table)
                self._table_name_map[rec.get("Name")] = cur_table

            elif rtype == SYSOBJ.Column:
                column = Column(rec.get("Id"), rec.get("Name"), JET_coltyp(rec.get("ColtypOrPgnoFDP")), record=rec)
                cur_table._add_column(column)

            elif rtype == SYSOBJ.Index:
                index = Index(cur_table, record=rec)
                cur_table._add_index(index)

            elif rtype == SYSOBJ.LongValue:
                cur_table._long_value_record = rec

            elif rtype == SYSOBJ.Callback:
                cur_table._long_callback_record = rec

    def table(self, name: str) -> Table:
        """Retrieve the table with the given name.

        Args:
            name: The table to retrieve.

        Raises:
            KeyError: If no table with that name exists.
        """
        try:
            return self._table_name_map[name]
        except KeyError:
            raise KeyError(f"No table with name: {name}")
