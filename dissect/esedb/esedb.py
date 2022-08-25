# Extensible Storage Engine (ESE) Database implementation
# Combination of pre-source release reverse engineering and post-source release cleanup
# Reference: https://github.com/microsoft/Extensible-Storage-Engine

from functools import cached_property, lru_cache
from typing import BinaryIO, Iterator

from dissect.esedb.page import Page
from dissect.esedb.table import Catalog, Table
from dissect.esedb.c_esedb import c_esedb, pgnoFDPMSO, ulDAEMagic
from dissect.esedb.exceptions import InvalidDatabase


class EseDB:
    """EseDB class.

    Loads an ESE database from the given file handle. Optionally enable impacket compatible data output.

    Impacket compatibility limits what values are parsed and returns most values as a hex string. Most notably,
    long and multi values are not parsed.

    Args:
        fh: The file-like object to open an ESE database on.
        impacket_compat: Whether to make the output impacket compatible.

    Raises:
        InvalidDatabase: If the file-like object does not look like an ESE database.
    """

    def __init__(self, fh: BinaryIO, impacket_compat: bool = False):
        self.fh = fh
        self.impacket_compat = impacket_compat

        self.header = c_esedb.DBFILEHDR(fh)
        if self.header.ulMagic != ulDAEMagic:
            raise InvalidDatabase("invalid file header signature")

        self.page_size = self.header.cbPageSize
        self.version = self.header.ulVersion
        self.format_major = self.header.ulDaeUpdateMajor
        self.format_minor = self.header.ulDaeUpdateMinor

        if self.format_major < 9:
            raise InvalidDatabase("unsupported format revision")

        self.catalog = Catalog(self, pgnoFDPMSO)

    @cached_property
    def has_small_pages(self) -> bool:
        """Return whether this database has small pages (<= 8K)."""
        return self.page_size <= 8192

    def table(self, name: str) -> Table:
        """Get a table by name.

        Args:
            name: The table to retrieve.
        """
        return self.catalog.table(name)

    def tables(self) -> list[Table]:
        """Get a list of all tables."""
        return self.catalog.tables

    def read_page(self, num: int) -> bytes:
        """Get the physical page data.

        Args:
            num: The physical page number to retrieve.

        Raises:
            IndexError: If the page number is out of bounds.
        """
        if num < 1:
            raise IndexError("page number cannot be less than 1")

        self.fh.seek((num - 1) * self.page_size)
        buf = self.fh.read(self.page_size)

        if len(buf) != self.page_size:
            raise IndexError("page number exceeds file size")

        return buf

    @lru_cache(maxsize=4096)
    def page(self, num: int) -> Page:
        """Get a logical page.

        The first two pages in the file are the Header and Shadow Header pages.
        Logical pages start at physical page 2 (zero-indexed).

        Args:
            num: The logical page number to retrieve.
        """
        buf = self.read_page(num + 2)
        return Page(self, num, buf)

    def pages(self) -> Iterator[Page]:
        """Iterate over all pages."""
        num = 1
        while True:
            try:
                yield self.page(num)
                num += 1
            except IndexError:
                break
