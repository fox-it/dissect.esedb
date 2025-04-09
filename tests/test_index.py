from __future__ import annotations

from typing import BinaryIO

from dissect.esedb.esedb import EseDB


def test_index(index_db: BinaryIO) -> None:
    db = EseDB(index_db)
    table = db.table("index")

    assert len(table.indexes) == 19

    assert table.indexes[0].name == "IxId"
    assert table.indexes[0].column_ids == [1]
    record = table.indexes[0].search(Id=1)
    assert record.Id == 1

    assert table.indexes[1].name == "IxBit"
    assert table.indexes[1].column_ids == [2]
    record = table.indexes[1].search(Bit=False)
    assert record.Bit is False

    assert table.indexes[2].name == "IxUnsignedByte"
    assert table.indexes[2].column_ids == [3]
    record = table.indexes[2].search(UnsignedByte=213)
    assert record.UnsignedByte == 213

    assert table.indexes[3].name == "IxShort"
    assert table.indexes[3].column_ids == [4]
    record = table.indexes[3].search(Short=-1337)
    assert record.Short == -1337

    assert table.indexes[4].name == "IxLong"
    assert table.indexes[4].column_ids == [5]
    record = table.indexes[4].search(Long=-13371337)
    assert record.Long == -13371337

    assert table.indexes[5].name == "IxCurrenc"
    assert table.indexes[5].column_ids == [6]
    record = table.indexes[5].search(Currency=1337133713371337)
    assert record.Currency == 1337133713371337

    assert table.indexes[6].name == "IxIEEESingle"
    assert table.indexes[6].column_ids == [7]
    record = table.indexes[6].search(IEEESingle=1.0)
    assert record.IEEESingle == 1.0

    assert table.indexes[7].name == "IxIEEEDouble"
    assert table.indexes[7].column_ids == [8]
    record = table.indexes[7].search(IEEEDouble=13371337.13371337)
    assert record.IEEEDouble == 13371337.13371337

    assert table.indexes[8].name == "IxDateTime"
    assert table.indexes[8].column_ids == [9]
    record = table.indexes[8].search(DateTime=4675210852477960192)
    assert record.DateTime == 4675210852477960192

    assert table.indexes[9].name == "IxUnsignedLong"
    assert table.indexes[9].column_ids == [10]
    record = table.indexes[9].search(UnsignedLong=13371337)
    assert record.UnsignedLong == 13371337

    assert table.indexes[10].name == "IxLongLong"
    assert table.indexes[10].column_ids == [11]
    record = table.indexes[10].search(LongLong=-13371337)
    assert record.LongLong == -13371337

    assert table.indexes[11].name == "IxGUID"
    assert table.indexes[11].column_ids == [12]
    record = table.indexes[11].search(GUID="3f360af1-6766-46dc-9af2-0dacf295c2a1")
    assert record.GUID == "3f360af1-6766-46dc-9af2-0dacf295c2a1"

    assert table.indexes[12].name == "IxUnsignedShort"
    assert table.indexes[12].column_ids == [13]
    record = table.indexes[12].search(UnsignedShort=1337)
    assert record.UnsignedShort == 1337

    assert table.indexes[13].name == "IxBinary"
    assert table.indexes[13].column_ids == [128]
    record = table.indexes[13].search(Binary=b"test binary data")
    assert record.Binary == b"test binary data"

    assert table.indexes[14].name == "IxLongBinary"
    assert table.indexes[14].column_ids == [256]
    record = table.indexes[14].search(LongBinary=b"test long binary data " + (b"a" * 1000))
    assert record.LongBinary == b"test long binary data " + (b"a" * 1000)

    assert table.indexes[15].name == "IxASCII"
    assert table.indexes[15].column_ids == [129]
    record = table.indexes[15].search(ASCII="Simple ASCII text")
    assert record.ASCII == "Simple ASCII text"

    assert table.indexes[16].name == "IxUnicode"
    assert table.indexes[16].column_ids == [130]
    record = table.indexes[16].search(Unicode="Simple Unicode text 🦊")
    assert record.Unicode == "Simple Unicode text 🦊"

    assert table.indexes[17].name == "IxLongASCII"
    assert table.indexes[17].column_ids == [257]
    record = table.indexes[17].search(LongASCII="Long ASCII text " + ("a" * 1024))
    assert record.LongASCII == "Long ASCII text " + ("a" * 1024)

    assert table.indexes[18].name == "IxLongUnicode"
    assert table.indexes[18].column_ids == [258]
    record = table.indexes[18].search(LongUnicode="Long Unicode text 🦊 " + ("a" * 1024))
    assert record.LongUnicode == "Long Unicode text 🦊 " + ("a" * 1024)
