import runpy
import sys
from typing import Any, Iterator

from dissect.esedb import EseDB
from dissect.esedb.c_esedb import RecordValue
from dissect.esedb.record import Record

try:
    from impacket import ese
except ImportError:
    print("This utility monkeypatches ESENT_DB from impacket, but impacket is not installed", file=sys.stderr)
    exit(1)


class ESENT_DB:
    def __init__(self, fileName: Any, pageSize: int = 8192, isRemote: bool = False) -> None:
        # I don't know what ``isRemote`` actually does so mimick what Impacket does
        if isRemote:
            self.fh = fileName
            self.fh.open()
        else:
            self.fh = open(fileName, "rb")
        self.db = EseDB(self.fh, impacket_compat=True)

    def openTable(self, tableName: str) -> Iterator[Record]:
        # Impacket only ever asks for the "next" record, so our cursor can simply be the record iterator
        return self.db.table(tableName).records()

    def getNextRow(self, cursor: Iterator[Record], filter_tables: list[str] = None):
        # Impacket uses a list to filter column names to make parsing a bit more efficient, but our parsing already
        # skips columns you don't request the value for
        try:
            # Impacket treats column names as bytes so we need to wrap all records to translate column lookup
            return RecordWrapper(next(cursor))
        except StopIteration:
            return None

    def close(self):
        self.fh.close()


class RecordWrapper:
    def __init__(self, record: Record):
        self._record = record

    def __getitem__(self, attr: bytes) -> RecordValue:
        # Impacket treats column names as bytes so decode to string
        return self._record.get(attr.decode())


ese.ESENT_DB = ESENT_DB


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(
            "Usage: python -m dissect.esedb.tools.impacket /path/to/impacket/secretsdump.py <arguments>",
            file=sys.stderr,
        )
        exit(1)

    del sys.argv[0]
    main_globals = sys.modules["__main__"].__dict__
    runpy.run_path(sys.argv[0], main_globals, "__main__")
