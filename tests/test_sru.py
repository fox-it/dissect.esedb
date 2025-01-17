from __future__ import annotations

from typing import BinaryIO

from dissect.esedb.tools.sru import SRU


def test_sru(sru_db: BinaryIO) -> None:
    db = SRU(sru_db)

    records = list(db.entries())
    assert len(records) == 220
