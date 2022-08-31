from __future__ import annotations

import argparse
from typing import BinaryIO, Iterator, Optional

from dissect.util.sid import read_sid
from dissect.util.ts import wintimestamp, oatimestamp

from dissect.esedb.c_esedb import RecordValue
from dissect.esedb.esedb import EseDB
from dissect.esedb.record import Record, serialise_record_column_values
from dissect.esedb.table import Table


NATIVE_TYPE_MAP = {
    "{DD6636C4-8929-4683-974E-22C046A43763}": {"ConnectStartTime": wintimestamp},
    "{5C8CF1C7-7257-4F13-B223-970EF5939312}": {"EndTime": wintimestamp},
}

SKIP_TABLES = [
    "MSysObjects",
    "MSysObjectsShadow",
    "MSysObjids",
    "MSysLocales",
    "SruDbIdMapTable",
    "SruDbCheckpointTable",
]


NAME_TO_GUID_MAP = {
    "network_data": "{973F5D5C-1D90-4944-BE8E-24B94231A174}",
    "network_connectivity": "{DD6636C4-8929-4683-974E-22C046A43763}",
    "energy_estimator": "{DA73FB89-2BEA-4DDC-86B8-6E048C6DA477}",
    "energy_usage": "{FEE4E14F-02A9-4550-B5CE-5FA2DA202E37}",
    "energy_usage_lt": "{FEE4E14F-02A9-4550-B5CE-5FA2DA202E37}LT",
    "application": "{D10CA2FE-6FCF-4F6D-848E-B2E99266FA89}",
    "push_notifications": "{D10CA2FE-6FCF-4F6D-848E-B2E99266FA86}",
    "application_timeline": "{5C8CF1C7-7257-4F13-B223-970EF5939312}",
    "vfu": "{7ACBBAA3-D029-4BE4-9A7A-0885927F1D8F}",
    # Refs:
    # - http://dfir.pro/index.php?link_id=92259 and
    # - HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion\SRUM\Extensions
    "sdp_volume_provider": "{17F4D97B-F26A-5E79-3A82-90040A47D13D}",
    "sdp_physical_disk_provider": "{841A7317-3805-518B-C2EA-AD224CB4AF84}",
    "sdp_cpu_provider": "{DC3D3B50-BB90-5066-FA4E-A5F90DD8B677}",
    "sdp_network_provider": "{EEE2F477-0659-5C47-EF03-6D6BEFD441B3}",
    # not seen in the wild
    "sdp_perf_count_provider": "{38AD6548-9313-58F8-45C7-D293BAFDC879}",
    "sdp_event_log_provider": "{CDF8EBF6-7C0F-5AC2-158F-DBFBEE981152}",
}


class SRU:
    def __init__(self, fh: BinaryIO):
        self.esedb = EseDB(fh)

        id_map_table = self.esedb.table("SruDbIdMapTable")
        self.id_map = {r.get("IdIndex"): r for r in id_map_table.records()}

    def get_table(self, table_name: str = None, table_guid: str = None) -> Optional[Table]:
        if all((table_name, table_guid)) or not any((table_name, table_guid)):
            raise ValueError("Either table_name or table_guid must be provided")

        if table_name and table_name not in NAME_TO_GUID_MAP:
            raise ValueError(f"Unknown table name: {table_name}")

        table_guid = table_guid or NAME_TO_GUID_MAP[table_name]

        try:
            return self.esedb.table(table_guid)
        except KeyError:
            return None

    def entries(self) -> Iterator[Entry]:
        for t in self.esedb.tables():
            if t.name in SKIP_TABLES:
                continue
            yield from self.get_table_entries(table=t)

    __iter__ = entries

    def get_table_entries(self, table: Table = None, table_name: str = None, table_guid: str = None) -> Iterator[Entry]:
        table = table or self.get_table(table_name=table_name, table_guid=table_guid)
        if not table:
            return
        for record in table.records():
            yield Entry(self, table, record)

    def resolve_id(self, value: int) -> Optional[str]:
        try:
            record = self.id_map[value]
        except KeyError:
            raise IndexError(value)

        if not record.get("IdBlob"):
            return None

        if record.get("IdType") in (0, 1, 2):
            return record.get("IdBlob").decode("utf-16-le").rstrip("\x00")
        else:
            return read_sid(record.get("IdBlob"))


class Entry:
    def __init__(self, sru: SRU, table: Table, record: Record):
        self.sru = sru
        self.table = table
        self.record = record
        self.provider = table.name

    def _get(self, attr: str) -> RecordValue:
        value = self.record.get(attr)
        if value is None:
            return value

        if attr in ("AppId", "UserId"):
            value = self.sru.resolve_id(value)

        if attr == "TimeStamp":
            value = oatimestamp(value)

        type_map = NATIVE_TYPE_MAP.get(self.table.name, None)
        if type_map and attr in type_map:
            value = type_map[attr](value)

        # Application timeline ({5C8CF1C7-7257-4F13-B223-970EF5939312}) seems to have a weird null value
        # (******** in decimal)
        if self.table.name == "{5C8CF1C7-7257-4F13-B223-970EF5939312}" and value in (0x2A2A2A2A2A2A2A2A, 0x2A2A2A2A):
            value = None

        return value

    def __getitem__(self, attr: str) -> RecordValue:
        return self._get(attr)

    def __getattr__(self, attr: str) -> RecordValue:
        try:
            return self._get(attr)
        except Exception:
            return object.__getattribute__(self, attr)

    def __repr__(self) -> str:
        column_values = serialise_record_column_values(self.record)
        return f"<Entry provider={self.table.name} {column_values}>"


def main():
    parser = argparse.ArgumentParser(description="dissect.esedb SRU parser")
    parser.add_argument("input", help="SRU database to read")
    parser.add_argument("-p", "--provider", help="filter records from this provider")
    args = parser.parse_args()

    with open(args.input, "rb") as fh:
        parser = SRU(fh)

        if args.provider in NAME_TO_GUID_MAP:
            for e in parser.get_table_entries(table_name=args.provider):
                print(e)
        elif args.provider:
            for e in parser.get_table_entries(table_guid=args.provider):
                print(e)
        else:
            for e in parser.entries():
                print(e)


if __name__ == "__main__":
    main()
