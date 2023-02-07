import datetime

from dissect.util.ts import oatimestamp

from dissect.esedb.c_esedb import JET_coltyp
from dissect.esedb.esedb import EseDB


def test_basic_types(basic_db):
    db = EseDB(basic_db)
    table = db.table("basic")

    assert [(col.name, col.type) for col in table.columns] == [
        ("Id", JET_coltyp.Long),
        ("Bit", JET_coltyp.Bit),
        ("UnsignedByte", JET_coltyp.UnsignedByte),
        ("Short", JET_coltyp.Short),
        ("Long", JET_coltyp.Long),
        ("Currency", JET_coltyp.Currency),
        ("IEEESingle", JET_coltyp.IEEESingle),
        ("IEEEDouble", JET_coltyp.IEEEDouble),
        ("DateTime", JET_coltyp.DateTime),
        ("UnsignedLong", JET_coltyp.UnsignedLong),
        ("LongLong", JET_coltyp.LongLong),
        ("GUID", JET_coltyp.GUID),
        ("UnsignedShort", JET_coltyp.UnsignedShort),
    ]

    records = list(table.records())
    assert len(records) == 2

    assert records[0].Id == 1
    assert records[0].Bit is False
    assert records[0].UnsignedByte == 213
    assert records[0].Short == -1337
    assert records[0].Long == -13371337
    assert records[0].Currency == 1337133713371337
    assert records[0].IEEESingle == 1.0
    assert records[0].IEEEDouble == 13371337.13371337
    assert oatimestamp(records[0].DateTime) == datetime.datetime(1999, 3, 1, 0, 0, tzinfo=datetime.timezone.utc)
    assert records[0].UnsignedLong == 13371337
    assert records[0].LongLong == -13371337
    assert records[0].GUID == "3f360af1-6766-46dc-9af2-0dacf295c2a1"
    assert records[0].UnsignedShort == 1337

    assert records[1].Id == 2
    assert records[1].Bit is True
    assert records[1].UnsignedByte == 255
    assert records[1].Short == 1339
    assert records[1].Long == 13391339
    assert records[1].Currency == -1339133913391339
    assert records[1].IEEESingle == -2.0
    assert records[1].IEEEDouble == -13391339.13391339
    assert oatimestamp(records[1].DateTime) == datetime.datetime(1337, 6, 9, 0, 0, tzinfo=datetime.timezone.utc)


def test_binary_types(binary_db):
    db = EseDB(binary_db)
    table = db.table("binary")

    assert [(col.name, col.type) for col in table.columns] == [
        ("Id", JET_coltyp.Long),
        ("FixedBinary", JET_coltyp.Binary),
        ("NullableFixedBinary", JET_coltyp.Binary),
        ("Binary", JET_coltyp.Binary),
        ("NullableBinary", JET_coltyp.Binary),
        ("MaxBinary", JET_coltyp.Binary),
        ("TaggedBinary", JET_coltyp.Binary),
        ("NullableTaggedBinary", JET_coltyp.Binary),
        ("LongBinary", JET_coltyp.LongBinary),
        ("LongCompressedBinary", JET_coltyp.LongBinary),
        ("MaxLongBinary", JET_coltyp.LongBinary),
        ("MaxLongCompressedBinary", JET_coltyp.LongBinary),
    ]

    records = list(table.records())
    assert len(records) == 1

    assert records[0].Id == 1
    assert records[0].FixedBinary == b"test fixed binary data" + (b"\x00" * 233)
    assert records[0].NullableFixedBinary is None
    assert records[0].Binary == b"test binary data"
    assert records[0].NullableBinary is None
    assert records[0].MaxBinary == b"test max binary data " + (b"a" * 70)
    assert records[0].TaggedBinary == b"test tagged binary data"
    assert records[0].NullableTaggedBinary is None
    assert records[0].LongBinary == b"test long binary data " + (b"a" * 1000)
    assert records[0].LongCompressedBinary == b"test long compressed binary data " + (b"a" * 1000)
    assert records[0].MaxLongBinary == b"test max long binary data " + (b"a" * 900)
    assert records[0].MaxLongCompressedBinary == b"test max long compressed binary data " + (b"a" * 900)


def test_text_types(text_db):
    db = EseDB(text_db)
    table = db.table("text")

    assert [(col.name, col.type) for col in table.columns] == [
        ("Id", JET_coltyp.Long),
        ("FixedASCII", JET_coltyp.Text),
        ("FixedUnicode", JET_coltyp.Text),
        ("NullableFixedASCII", JET_coltyp.Text),
        ("NullableFixedUnicode", JET_coltyp.Text),
        ("ASCII", JET_coltyp.Text),
        ("Unicode", JET_coltyp.Text),
        ("NullableASCII", JET_coltyp.Text),
        ("NullableUnicode", JET_coltyp.Text),
        ("MaxASCII", JET_coltyp.Text),
        ("MaxUnicode", JET_coltyp.Text),
        ("TaggedASCII", JET_coltyp.Text),
        ("TaggedUnicode", JET_coltyp.Text),
        ("NullableTaggedASCII", JET_coltyp.Text),
        ("NullableTaggedUnicode", JET_coltyp.Text),
        ("LongASCII", JET_coltyp.LongText),
        ("LongUnicode", JET_coltyp.LongText),
        ("LongCompressedASCII", JET_coltyp.LongText),
        ("LongCompressedUnicode", JET_coltyp.LongText),
        ("LongTinyASCII", JET_coltyp.LongText),
        ("LongTinyUnicode", JET_coltyp.LongText),
        ("LongTinyCompressedASCII", JET_coltyp.LongText),
        ("LongTinyCompressedUnicode", JET_coltyp.LongText),
        ("MaxLongASCII", JET_coltyp.LongText),
        ("MaxLongUnicode", JET_coltyp.LongText),
        ("MaxLongCompressedASCII", JET_coltyp.LongText),
        ("MaxLongCompressedUnicode", JET_coltyp.LongText),
    ]

    records = list(table.records())
    assert len(records) == 1

    assert records[0].Id == 1
    assert records[0].FixedASCII == "Fixed ASCII text" + (" " * 239)
    assert records[0].FixedUnicode == "Fixed Unicode text 🦊" + (" " * 107)
    assert records[0].NullableFixedASCII is None
    assert records[0].NullableFixedUnicode is None
    assert records[0].ASCII == "Simple ASCII text"
    assert records[0].Unicode == "Simple Unicode text 🦊"
    assert records[0].NullableASCII is None
    assert records[0].NullableUnicode is None
    assert records[0].MaxASCII == "Max ASCII text that can't be that long"
    assert records[0].MaxUnicode == "Max Unicode text that can't be that long 🦊"
    assert records[0].TaggedASCII == "Tagged ASCII text"
    assert records[0].TaggedUnicode == "Tagged Unicode text 🦊"
    assert records[0].NullableTaggedASCII is None
    assert records[0].NullableTaggedUnicode is None
    assert records[0].LongASCII == "Long ASCII text " + ("a" * 1024)
    assert records[0].LongUnicode == "Long Unicode text 🦊 " + ("a" * 1024)
    assert records[0].LongCompressedASCII == "Long compressed ASCII text " + ("a" * 1024)
    assert records[0].LongCompressedUnicode == "Long compressed Unicode text 🦊 " + ("a" * 1024)
    assert records[0].LongTinyASCII == "Tiny ASCII"
    assert records[0].LongTinyUnicode == "Tiny 🦊"
    assert records[0].LongTinyCompressedASCII == "Tiny c ASCII"
    assert records[0].LongTinyCompressedUnicode == "Tiny c 🦊"
    assert records[0].MaxLongASCII == "Max long ASCII text that can be a bit longer " + ("a" * 900)
    assert records[0].MaxLongUnicode == "Max long Unicode text that can be a bit longer 🦊 " + ("a" * 900)
    assert records[0].MaxLongCompressedASCII == "Max long compressed ASCII text that can be a bit longer " + ("a" * 900)
    assert records[0].MaxLongCompressedUnicode == "Max long compressed Unicode text that can be a bit longer 🦊 " + (
        "a" * 900
    )


def test_multivalue_types(multi_db):
    db = EseDB(multi_db)
    table = db.table("multi")

    assert [(col.name, col.type) for col in table.columns] == [
        ("Id", JET_coltyp.Long),
        ("Bit", JET_coltyp.Bit),
        ("UnsignedByte", JET_coltyp.UnsignedByte),
        ("Short", JET_coltyp.Short),
        ("Long", JET_coltyp.Long),
        ("Currency", JET_coltyp.Currency),
        ("IEEESingle", JET_coltyp.IEEESingle),
        ("IEEEDouble", JET_coltyp.IEEEDouble),
        ("DateTime", JET_coltyp.DateTime),
        ("Binary", JET_coltyp.Binary),
        ("LongBinary", JET_coltyp.LongBinary),
        ("LongCompressedBinary", JET_coltyp.LongBinary),
        ("ASCII", JET_coltyp.Text),
        ("Unicode", JET_coltyp.Text),
        ("LongASCII", JET_coltyp.LongText),
        ("LongUnicode", JET_coltyp.LongText),
        ("LongCompressedASCII", JET_coltyp.LongText),
        ("LongCompressedUnicode", JET_coltyp.LongText),
        ("UnsignedLong", JET_coltyp.UnsignedLong),
        ("LongLong", JET_coltyp.LongLong),
        ("GUID", JET_coltyp.GUID),
        ("UnsignedShort", JET_coltyp.UnsignedShort),
    ]

    records = list(table.records())
    assert len(records) == 2

    assert records[0].Id == 1
    assert records[0].Bit == [False, True]
    assert records[0].UnsignedByte == [0, 127, 255]
    assert records[0].Short == [0, -32767, 32767]
    assert records[0].Long == [0, -2147483647, 2147483647]
    assert records[0].Currency == [0, -9223372036854775807, 9223372036854775807]
    assert records[0].IEEESingle == [0.0, -1.0, 1.0]
    assert records[0].IEEEDouble == [0.0, -1.0, 1.0]
    assert list(map(oatimestamp, records[0].DateTime)) == [
        datetime.datetime(1661, 4, 17, 11, 30, tzinfo=datetime.timezone.utc),
        datetime.datetime(2077, 4, 1, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2517, 9, 24, 5, 30, tzinfo=datetime.timezone.utc),
    ]
    assert records[0].Binary == [
        b"Some binary data that has multiple values, this is value 1",
        b"Some binary data that has multiple values, this is value 2",
        b"Some binary data that has multiple values, this is value 3",
    ]
    assert records[0].LongBinary == [
        b"Some very long binary data that has multiple values, this is value 1 " + (b"a" * 1024),
        b"Some very long binary data that has multiple values, this is value 2 " + (b"a" * 1024),
        b"Some very long binary data that has multiple values, this is value 3 " + (b"a" * 1024),
    ]
    assert records[0].LongCompressedBinary == [
        b"Some very long compressed binary data that has multiple values, this is value 1 " + (b"a" * 1024),
        b"Some very long compressed binary data that has multiple values, this is value 2 " + (b"a" * 1024),
        b"Some very long compressed binary data that has multiple values, this is value 3 " + (b"a" * 1024),
    ]
    assert records[0].ASCII == [
        "Some ASCII text that has multiple values, this is value 1",
        "Some ASCII text that has multiple values, this is value 2",
        "Some ASCII text that has multiple values, this is value 3",
    ]
    assert records[0].Unicode == [
        "Some Unicode text that has multiple values, this is value 1 🦊",
        "Some Unicode text that has multiple values, this is value 2 🦊🦊",
        "Some Unicode text that has multiple values, this is value 3 🦊🦊🦊",
    ]
    assert records[0].LongASCII == [
        "Some very long ASCII text that has multiple values, this is value 1 " + ("a" * 1024),
        "Some very long ASCII text that has multiple values, this is value 2 " + ("a" * 1024),
        "Some very long ASCII text that has multiple values, this is value 3 " + ("a" * 1024),
    ]
    assert records[0].LongUnicode == [
        "Some very long Unicode text that has multiple values, this is value 1 🦊 " + ("a" * 1024),
        "Some very long Unicode text that has multiple values, this is value 2 🦊🦊 " + ("a" * 1024),
        "Some very long Unicode text that has multiple values, this is value 3 🦊🦊🦊 " + ("a" * 1024),
    ]
    assert records[0].LongCompressedASCII == [
        "Some very long compressed ASCII text that has multiple values, this is value 1 " + ("a" * 1024),
        "Some very long compressed ASCII text that has multiple values, this is value 2 " + ("a" * 1024),
        "Some very long compressed ASCII text that has multiple values, this is value 3 " + ("a" * 1024),
    ]
    assert records[0].LongCompressedUnicode == [
        "Some very long compressed Unicode text that has multiple values, this is value 1 🦊 " + ("a" * 1024),
        "Some very long compressed Unicode text that has multiple values, this is value 2 🦊🦊 " + ("a" * 1024),
        "Some very long compressed Unicode text that has multiple values, this is value 3 🦊🦊🦊 " + ("a" * 1024),
    ]
    assert records[0].UnsignedLong == [0, 4294967295]
    assert records[0].LongLong == [0, -9223372036854775807, 9223372036854775807]
    assert records[0].GUID == [
        "03402861-fad3-4ce5-986e-d31df852f2a7",
        "09b589e3-a92b-4936-bbc4-bb9dff334bf3",
        "2212ff6a-6712-4fe4-bb4a-21e6d0e043d1",
    ]
    assert records[0].UnsignedShort == [0, 65535]

    # Test some small "long" multi-values and two-values
    # Also test that the other columns should correctly be set to None
    assert records[1].Id == 2
    assert records[1].Bit is None
    assert records[1].UnsignedByte is None
    assert records[1].Short is None
    assert records[1].Long is None
    assert records[1].Currency is None
    assert records[1].IEEESingle is None
    assert records[1].IEEEDouble is None
    assert records[1].DateTime is None
    assert records[1].Binary is None
    assert records[1].LongBinary == [
        b"Tiny binary 1",
        b"Tiny binary 2",
        b"Tiny binary 3",
    ]
    assert records[1].LongCompressedBinary == [
        b"Tiny c binary 1",
        b"Tiny c binary 2",
        b"Tiny c binary 3",
    ]
    assert records[1].ASCII is None
    assert records[1].Unicode is None
    assert records[1].LongASCII == [
        "Tiny ASCII 1",
        "Tiny ASCII 2",
    ]
    assert records[1].LongUnicode == [
        "Tiny 🦊 1",
        "Tiny 🦊🦊",
        "Tiny 🦊🦊🦊",
    ]
    assert records[1].LongCompressedASCII == [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "ccccccccccccccccccccccccccccccccccc",
    ]
    assert records[1].LongCompressedUnicode == [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 🦊",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 🦊🦊",
    ]
    assert records[1].UnsignedLong is None
    assert records[1].LongLong is None
    assert records[1].GUID is None
    assert records[1].UnsignedShort is None


def test_default_db(default_db):
    db = EseDB(default_db)
    table = db.table("default")

    records = list(table.records())
    assert len(records) == 1

    assert records[0].Id == 1
    assert records[0].Bit is True
    assert records[0].UnsignedByte == 69
    assert records[0].Short == 0x1234
    assert records[0].Long == 0x12345678
    assert records[0].Currency == 0x123456789ABCDEF0
    assert records[0].IEEESingle == 1.0
    assert records[0].IEEEDouble == 2.0
    assert oatimestamp(records[0].DateTime) == datetime.datetime(2022, 10, 4, 0, 0, tzinfo=datetime.timezone.utc)
    assert records[0].UnsignedLong == 12345678
    assert records[0].LongLong == 0xC001DEADD00D
    assert records[0].GUID == "c001d00d-dead-beef-face-feeddeadbeef"
    assert records[0].UnsignedShort == 0xF00D

    assert records[0].Binary == b"Short default binary"
    assert records[0].LongBinary == b"Long default binary " + (b"a" * 200)
    assert records[0].ASCII == "Short default ASCII"
    assert records[0].Unicode == "Short default Unicode 🦊"
    assert records[0].LongASCII == "Long default ASCII " + ("a" * 200)
    assert records[0].LongUnicode == "Long default Unicode 🦊 " + ("a" * 64)


def test_large_db(large_db):
    db = EseDB(large_db)
    table = db.table("large")

    assert db.page_size == 32 * 1024
    assert [(col.name, col.type) for col in table.columns] == [
        ("Id", JET_coltyp.Long),
    ] + [(f"Column{i}", JET_coltyp.UnsignedShort) for i in range(64993)]

    records = list(table.records())
    assert len(records) == 16

    for i in range(64993):
        assert records[i // 4096].get(f"Column{i}") == i
