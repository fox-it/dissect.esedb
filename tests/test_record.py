from typing import BinaryIO

from dissect.esedb.esedb import EseDB


def test_as_dict(basic_db: BinaryIO):
    db = EseDB(basic_db)
    table = db.table("basic")

    records = list(table.records())
    assert len(records) == 2

    assert [r.as_dict() for r in records] == [
        {
            "Id": 1,
            "Bit": False,
            "UnsignedByte": 213,
            "Short": -1337,
            "Long": -13371337,
            "Currency": 1337133713371337,
            "IEEESingle": 1.0,
            "IEEEDouble": 13371337.13371337,
            "DateTime": 4675210852477960192,
            "UnsignedLong": 13371337,
            "LongLong": -13371337,
            "GUID": "3f360af1-6766-46dc-9af2-0dacf295c2a1",
            "UnsignedShort": 1337,
        },
        {
            "Id": 2,
            "Bit": True,
            "UnsignedByte": 255,
            "Short": 1339,
            "Long": 13391339,
            "Currency": -1339133913391339,
            "IEEESingle": -2.0,
            "IEEEDouble": -13391339.13391339,
            "DateTime": -4537072128574357504,
        },
    ]
