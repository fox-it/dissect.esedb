from unittest.mock import Mock

from dissect.esedb.c_esedb import c_esedb
from dissect.esedb.page import Page


def test_tag_state() -> None:
    mock_esedb = Mock()
    mock_esedb.has_small_pages = True

    mock_header = c_esedb.PGHDR(
        itagState=0x2069,
    ).dumps()

    page = Page(mock_esedb, 0, mock_header)
    assert page.tag_reserved == 2
    assert page.tag_count == 0x69

    mock_header = c_esedb.PGHDR(
        itagState=0x0069,
    ).dumps()

    page = Page(mock_esedb, 0, mock_header)
    assert page.tag_reserved == 1
    assert page.tag_count == 0x69
