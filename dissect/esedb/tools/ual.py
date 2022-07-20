import ipaddress
import datetime

from dissect.esedb.esedb import EseDB
from dissect.esedb.exceptions import InvalidTable

from dissect.util.ts import wintimestamp


SKIP_TABLES = [
    "MSysObjects",
    "MSysObjectsShadow",
    "MSysObjids",
    "MSysLocales",
]


class UalParser:

    WIN_DATETIME_FIELDS = (
        "CreationTime",
        "FirstSeen",
        "InsertDate",
        "LastAccess",
        "LastSeen",
    )

    def __init__(self, fh):
        self.esedb = EseDB(fh)

    def get_tables(self):
        return self.esedb.tables()

    def get_table_records(self, table_name):
        try:
            table = self.esedb.table(table_name)
        except InvalidTable:
            return None

        for record in table.get_records():
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


def convert_day_num_to_date(year, day_num):
    return datetime.datetime(year, 1, 1) + datetime.timedelta(day_num - 1)
