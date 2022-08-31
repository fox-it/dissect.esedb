from dissect.esedb.tools.sru import SRU


def test_sru(sru_db):
    db = SRU(sru_db)

    records = list(db.entries())
    assert len(records) == 220
