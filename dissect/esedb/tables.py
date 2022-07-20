import struct
from typing import Generator, List

from dissect.esedb.exceptions import InvalidTable
from dissect.esedb.records import Record
from dissect.esedb.utils import COLUMN_TYPE


class Column:
    def __init__(self, identifier, name, column_type, record=None):
        self.identifier = identifier
        self.type = column_type
        self.name = name
        self.record = record

    def __repr__(self):
        return f"<Column name={self.name} identifier=0x{self.identifier:x} type={self.type}>"

    def is_uuid(self) -> bool:
        return self.type == COLUMN_TYPE.JET_coltypGUID


class Table:
    def __init__(self, esedb, name, root_page, columns=None, indices=None, record=None):
        self.esedb = esedb

        self.name = name
        self.indices = indices or []
        self.columns = columns or []
        self.column_name_map = {c.name: c for c in self.columns}
        self.long_value_record = None
        self.long_callback_record = None

        self.record = record
        self.root = esedb.page(root_page)

    def __repr__(self):
        return f"<Table name={self.name}>"

    def get_records(self) -> Generator[Record, None, None]:
        for node in self.root.walk():
            yield Record(self, node.value)

    def get_column(self, column_name) -> Column:
        return self.column_name_map.get(column_name)

    def add_index(self, index):
        self.indices.append(index)

    def add_column(self, column):
        self.columns.append(column)
        self.column_name_map[column.name] = column

    @property
    def column_names(self) -> List[str]:
        return list(self.column_name_map.keys())

    def get_long_value(self, key) -> bytes:
        rkey = key[::-1]
        long_value_tree = self.esedb.page(self.long_value_record.get("ColtypOrPgnoFDP"))
        header = long_value_tree.find_key(rkey)

        buf = []
        offset = 0
        unk, size = struct.unpack("<2I", header.value)
        while offset < size:
            lkey = rkey + struct.pack("<I", offset)
            leaf = long_value_tree.find_key(lkey)

            buf.append(leaf.value)
            offset += len(leaf.value)

        return b"".join(buf)


class Index:
    def __init__(self, esedb, record=None):
        self.esedb = esedb
        self.record = record


class Catalog:

    CATALOG_COLUMNS = [
        Column(1, "ObjidTable", COLUMN_TYPE.JET_coltypLong),
        Column(2, "Type", COLUMN_TYPE.JET_coltypShort),
        Column(3, "Id", COLUMN_TYPE.JET_coltypLong),
        Column(4, "ColtypOrPgnoFDP", COLUMN_TYPE.JET_coltypLong),
        Column(5, "SpaceUsage", COLUMN_TYPE.JET_coltypLong),
        Column(6, "Flags", COLUMN_TYPE.JET_coltypLong),
        Column(7, "PagesOrLocale", COLUMN_TYPE.JET_coltypLong),
        Column(8, "RootFlag", COLUMN_TYPE.JET_coltypBit),
        Column(9, "RecordOffset", COLUMN_TYPE.JET_coltypShort),
        Column(10, "LCMapFlags", COLUMN_TYPE.JET_coltypLong),
        Column(11, "KeyMost", COLUMN_TYPE.JET_coltypUnsignedShort),
        Column(12, "LVChunkMax", COLUMN_TYPE.JET_coltypLong),
        Column(128, "Name", COLUMN_TYPE.JET_coltypText),
        Column(129, "Stats", COLUMN_TYPE.JET_coltypBinary),
        Column(130, "TemplateTable", COLUMN_TYPE.JET_coltypText),
        Column(131, "DefaultValue", COLUMN_TYPE.JET_coltypBinary),
        Column(132, "KeyFldIDs", COLUMN_TYPE.JET_coltypBinary),
        Column(133, "VarSegMac", COLUMN_TYPE.JET_coltypBinary),
        Column(134, "ConditionalColumns", COLUMN_TYPE.JET_coltypBinary),
        Column(135, "TupleLimits", COLUMN_TYPE.JET_coltypBinary),
        Column(136, "Version", COLUMN_TYPE.JET_coltypBinary),
        Column(137, "SortID", COLUMN_TYPE.JET_coltypBinary),
        Column(256, "CallbackData", COLUMN_TYPE.JET_coltypLongBinary),
        Column(257, "CallbackDependencies", COLUMN_TYPE.JET_coltypLongBinary),
        Column(258, "SeparateLV", COLUMN_TYPE.JET_coltypLongBinary),
        Column(259, "SpaceHints", COLUMN_TYPE.JET_coltypLongBinary),
        Column(260, "SpaceDeferredLVHints", COLUMN_TYPE.JET_coltypLongBinary),
        Column(261, "LocaleName", COLUMN_TYPE.JET_coltypLongBinary),
    ]

    CATALOG_TYPE_TABLE = 1
    CATALOG_TYPE_COLUMN = 2
    CATALOG_TYPE_INDEX = 3
    CATALOG_TYPE_LONG_VALUE = 4
    CATALOG_TYPE_LONG_CALLBACK = 5

    def __init__(self, esedb, root_page):
        self.esedb = esedb
        self.root_page = root_page
        self.tables = []
        self.table_name_map = {}

        # Create a dummy table with a predetermined list of columns
        catalog_table = Table(esedb, None, root_page, columns=self.CATALOG_COLUMNS)

        table_id_map = {}

        for record in catalog_table.get_records():
            record_type = record.get("Type")

            if record_type == self.CATALOG_TYPE_TABLE:
                table = Table(self.esedb, record.get("Name"), record.get("ColtypOrPgnoFDP"), record=record)
                self.tables.append(table)

                self.table_name_map[record.get("Name")] = table
                table_id_map[record.get("ObjidTable")] = table
                continue
            else:
                table = table_id_map[record.get("ObjidTable")]

            if record_type == self.CATALOG_TYPE_COLUMN:
                column = Column(
                    record.get("Id"),
                    record.get("Name"),
                    COLUMN_TYPE(record.get("ColtypOrPgnoFDP")),
                    record=record,
                )
                table.add_column(column)
            elif record_type == self.CATALOG_TYPE_INDEX:
                table.add_index(Index(self.esedb, record=record))
            elif record_type == self.CATALOG_TYPE_LONG_VALUE:
                table.long_value_record = record
            elif record_type == self.CATALOG_TYPE_LONG_CALLBACK:
                table.long_callback_record = record

    def get_table(self, name) -> Table:
        if name not in self.table_name_map:
            raise InvalidTable(f"No table with name: {name}")
        return self.table_name_map[name]

    def get_tables(self) -> List[Table]:
        return self.tables
