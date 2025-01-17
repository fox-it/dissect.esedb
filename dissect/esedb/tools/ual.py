import argparse
import datetime
import ipaddress
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Union

from dissect.util.ts import wintimestamp

from dissect.esedb.c_esedb import RecordValue
from dissect.esedb.esedb import EseDB
from dissect.esedb.table import Table

UalValue = Union[RecordValue, ipaddress.IPv4Address, ipaddress.IPv6Interface, tuple[datetime.datetime]]

SKIP_TABLES = [
    "MSysObjects",
    "MSysObjectsShadow",
    "MSysObjids",
    "MSysLocales",
]


class UAL:
    WIN_DATETIME_FIELDS = (
        "CreationTime",
        "FirstSeen",
        "InsertDate",
        "LastAccess",
        "LastSeen",
    )

    def __init__(self, fh: BinaryIO):
        self.esedb = EseDB(fh)

    def get_tables(self) -> list[Table]:
        return self.esedb.tables()

    def get_table_records(self, table_name: str) -> Iterator[dict[str, UalValue]]:
        try:
            table = self.esedb.table(table_name)
        except KeyError:
            return None

        for record in table.records():
            record_data = {}

            last_access_year = None
            day_counts = {}

            for column in table.columns:
                value = record.get(column.name)

                if column.name in self.WIN_DATETIME_FIELDS:
                    value = wintimestamp(value)

                if column.name == "LastAccess":
                    last_access_year = value.year

                if column.name == "Address" and isinstance(value, bytes):
                    value = ipaddress.ip_address(value)
                    value = str(value)

                if column.name.startswith("Day"):
                    day_num = int(column.name[3:])
                    day_counts[day_num] = value
                    continue

                record_data[column.name] = value

            if day_counts:
                if last_access_year:
                    # drop days without counts and convert day number to date
                    day_counts = {
                        convert_day_num_to_date(last_access_year, day_num): count
                        for day_num, count in day_counts.items()
                        if count
                    }
                else:
                    day_counts = {}
                record_data["activity_counts"] = tuple(day_counts.items())

            yield record_data


def convert_day_num_to_date(year: int, day_num: int) -> datetime.datetime:
    return datetime.datetime(year, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(day_num - 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="dissect.esedb UAL parser")
    parser.add_argument("input", help="UAL database to read")
    args = parser.parse_args()

    with Path(args.input).open("rb") as fh:
        parser = UAL(fh)

        for table in parser.get_tables():
            if table.name in SKIP_TABLES:
                continue

            for record in parser.get_table_records(table.name):
                print(record)


if __name__ == "__main__":
    main()
