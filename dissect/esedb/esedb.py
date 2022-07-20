""" Extensible Storage Engine (ESE) Database """

from functools import lru_cache

from dissect.esedb.pages import Page
from dissect.esedb.tables import Catalog
from dissect.esedb.exceptions import InvalidDatabase, InvalidPageNumber
from dissect.esedb.utils import c_esedb, CATALOG_PAGE_NUMBER


class EseDB:
    def __init__(self, fh, impacket_compat=False):
        self.fh = fh
        self.impacket_compat = impacket_compat

        self.header = c_esedb.esedb_file_header(fh)
        if self.header.signature != b"\xef\xcd\xab\x89":
            raise InvalidDatabase("invalid signature")

        backup_offset = 0x0800
        while backup_offset <= 0x8000:
            fh.seek(backup_offset)
            backup_header = c_esedb.esedb_file_header(fh)
            if backup_header.signature == b"\xef\xcd\xab\x89":
                break
            backup_offset <<= 1
        else:
            raise InvalidDatabase("no backup header")

        self.backup_header = backup_header
        self.backup_offset = backup_offset

        self.page_size = self.header.page_size
        self.format_version = self.header.format_version
        self.format_revision = self.header.format_revision

        self.catalog = Catalog(self, CATALOG_PAGE_NUMBER)

    def table(self, name):
        """Get a table by name."""
        return self.catalog.get_table(name)

    def tables(self):
        """Get a list of all tables."""
        return self.catalog.get_tables()

    def read_page(self, num):
        """Get the physical page data."""
        if num < 1:
            raise InvalidPageNumber("page number exceeds boundaries")

        self.fh.seek((num - 1) * self.page_size)
        buf = self.fh.read(self.page_size)

        if not buf:
            raise InvalidPageNumber("page number exceeds boundaries")

        return buf

    @lru_cache(4096)
    def page(self, num):
        """Get logical page.

        The first two pages in the file are the Header and Shadow Header pages.
        Logical pages start at physical page 2 (zero-indexed).
        """

        buf = self.read_page(num + 2)
        return Page(self, num, buf)
