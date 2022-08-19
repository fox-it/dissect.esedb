from dissect.esedb.tools.ual import UAL


def test_ual(ual_db):
    db = UAL(ual_db)

    assert len(list(db.get_table_records("CLIENTS"))) == 19
    assert len(list(db.get_table_records("ROLE_ACCESS"))) == 3
    assert len(list(db.get_table_records("VIRTUALMACHINES"))) == 0
    assert len(list(db.get_table_records("DNS"))) == 12
    assert len(list(db.get_table_records("SYSTEM_IDENTITY"))) == 0
