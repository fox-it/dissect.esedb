from dissect.esedb.esedb import EseDB


def test_index(index_db):
    db = EseDB(index_db)
    table = db.table("index")

    assert len(table.indexes) == 19

    assert table.indexes[0].name == "IxId"
    assert table.indexes[0].column_ids == [1]
    assert table.indexes[0].search(Id=1)

    assert table.indexes[1].name == "IxBit"
    assert table.indexes[1].column_ids == [2]
    assert table.indexes[1].search(Bit=False)

    assert table.indexes[2].name == "IxUnsignedByte"
    assert table.indexes[2].column_ids == [3]
    assert table.indexes[2].search(UnsignedByte=213)

    assert table.indexes[3].name == "IxShort"
    assert table.indexes[3].column_ids == [4]
    assert table.indexes[3].search(Short=-1337)

    assert table.indexes[4].name == "IxLong"
    assert table.indexes[4].column_ids == [5]
    assert table.indexes[4].search(Long=-13371337)

    assert table.indexes[5].name == "IxCurrenc"
    assert table.indexes[5].column_ids == [6]
    assert table.indexes[5].search(Currency=1337133713371337)

    assert table.indexes[6].name == "IxIEEESingle"
    assert table.indexes[6].column_ids == [7]
    assert table.indexes[6].search(IEEESingle=1.0)

    assert table.indexes[7].name == "IxIEEEDouble"
    assert table.indexes[7].column_ids == [8]
    assert table.indexes[7].search(IEEEDouble=13371337.13371337)

    assert table.indexes[8].name == "IxDateTime"
    assert table.indexes[8].column_ids == [9]
    assert table.indexes[8].search(DateTime=4675210852477960192)

    assert table.indexes[9].name == "IxUnsignedLong"
    assert table.indexes[9].column_ids == [10]
    assert table.indexes[9].search(UnsignedLong=13371337)

    assert table.indexes[10].name == "IxLongLong"
    assert table.indexes[10].column_ids == [11]
    assert table.indexes[10].search(LongLong=-13371337)

    assert table.indexes[11].name == "IxGUID"
    assert table.indexes[11].column_ids == [12]
    assert table.indexes[11].search(GUID="3f360af1-6766-46dc-9af2-0dacf295c2a1")

    assert table.indexes[12].name == "IxUnsignedShort"
    assert table.indexes[12].column_ids == [13]
    assert table.indexes[12].search(UnsignedShort=1337)

    assert table.indexes[13].name == "IxBinary"
    assert table.indexes[13].column_ids == [128]
    assert table.indexes[13].search(Binary=b"test binary data")

    assert table.indexes[14].name == "IxLongBinary"
    assert table.indexes[14].column_ids == [256]
    assert table.indexes[14].search(LongBinary=b"test long binary data " + (b"a" * 1024))

    assert table.indexes[15].name == "IxASCII"
    assert table.indexes[15].column_ids == [129]
    assert table.indexes[15].search(ASCII="Simple ASCII text")

    assert table.indexes[16].name == "IxUnicode"
    assert table.indexes[16].column_ids == [130]
    assert table.indexes[16].search(Unicode="Simple Unicode text 🦊")

    assert table.indexes[17].name == "IxLongASCII"
    assert table.indexes[17].column_ids == [257]
    assert table.indexes[17].search(LongASCII="Long ASCII text " + ("a" * 1024))

    assert table.indexes[18].name == "IxLongUnicode"
    assert table.indexes[18].column_ids == [258]
    assert table.indexes[18].search(LongUnicode="Long Unicode text 🦊 " + ("a" * 1024))
