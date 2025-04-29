from typing import BinaryIO

from dissect.esedb.esedb import EseDB


def test_cursor(basic_db: BinaryIO) -> None:
    db = EseDB(basic_db)
    table = db.table("basic")
    idx = table.index("IxId")

    cursor = idx.cursor()
    record = cursor.search(Id=1)
    assert record.Id == 1
    record = cursor.next()
    assert record.Id == 2
    record = cursor.prev()
    assert record.Id == 1
    assert record.Id == cursor.record().Id


def test_cursor_iterator(basic_db: BinaryIO) -> None:
    db = EseDB(basic_db)
    table = db.table("basic")
    idx = table.index("IxId")

    cursor = idx.cursor()
    records = list(cursor)
    assert len(records) == 2
    assert records[0].Id == 1
    assert records[1].Id == 2


def test_cursor_search(ual_db: BinaryIO) -> None:
    db = EseDB(ual_db)
    table = db.table("CLIENTS")
    idx = table.index("Username_RoleGuid_TenantId_index")

    cursor = idx.cursor()
    records = list(
        cursor.find_all(
            AuthenticatedUserName="blackclover\\administrator",
            RoleGuid="ad495fc3-0eaa-413d-ba7d-8b13fa7ec598",
            TenantId="2417e4c3-5467-40c5-809b-12b59a86c102",
        )
    )

    assert len(records) == 5

    cursor.reset()
    records = list(
        cursor.find_all(
            AuthenticatedUserName="blackclover\\administrator",
            RoleGuid="ad495fc3-0eaa-413d-ba7d-8b13fa7ec598",
            TenantId="2417e4c3-5467-40c5-809b-12b59a86c102",
            Day204=4,
        )
    )

    assert len(records) == 1
