from __future__ import annotations

from typing import TYPE_CHECKING

from dissect.esedb.btree import BTree
from dissect.esedb.exceptions import NoNeighbourPageError
from dissect.esedb.record import Record

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dissect.esedb.c_esedb import RecordValue
    from dissect.esedb.index import Index
    from dissect.esedb.page import Node


class Cursor:
    """A simple cursor implementation for searching the ESE indexes.

    Args:
        index: The :class:`~dissect.esedb.index.Index` to create the cursor for.
    """

    def __init__(self, index: Index):
        self.index = index
        self.table = index.table
        self.esedb = index.esedb

        self._primary = BTree(self.esedb, index.root)
        self._secondary = None if index.is_primary else BTree(self.esedb, self.table.root)

    def __iter__(self) -> Iterator[Record]:
        while True:
            yield self._record()

            try:
                self._primary.next()
            except NoNeighbourPageError:
                break

    def _node(self) -> Node:
        """Return the node the cursor is currently on. Resolves the secondary index if needed.

        Returns:
            A :class:`~dissect.esedb.page.Node` object of the current node.
        """
        node = self._primary.node()
        if self._secondary:
            self._secondary.reset()
            node = self._secondary.search(node.data.tobytes(), exact=True)
        return node

    def _record(self) -> Record:
        """Return the record the cursor is currently on.

        Returns:
            A :class:`~dissect.esedb.record.Record` object of the current record.
        """
        return Record(self.table, self._node())

    def reset(self) -> None:
        """Reset the internal state."""
        self._primary.reset()
        if self._secondary:
            self._secondary.reset()

    def search(self, **kwargs: RecordValue) -> Record:
        """Search the index for the requested values.

        Searching modifies the cursor state. Searching again will search from the current position.
        Reset the cursor with :meth:`reset` to start from the beginning.

        Args:
            **kwargs: The columns and values to search for.

        Returns:
            A :class:`~dissect.esedb.record.Record` object of the found record.
        """
        key = self.index.make_key(kwargs)
        return self.search_key(key, exact=True)

    def search_key(self, key: bytes, exact: bool = True) -> Record:
        """Search for a record with the given ``key``.

        Args:
            key: The key to search for.
            exact: If ``True``, search for an exact match. If ``False``, sets the cursor on the
                   next record that is greater than or equal to the key.
        """
        self._primary.search(key, exact)
        return self._record()

    def seek(self, **kwargs: RecordValue) -> None:
        """Seek to the record with the given values.

        Args:
            **kwargs: The columns and values to seek to.
        """
        key = self.index.make_key(kwargs)
        self.search_key(key, exact=False)

    def seek_key(self, key: bytes) -> None:
        """Seek to the record with the given ``key``.

        Args:
            key: The key to seek to.
        """
        self._primary.search(key, exact=False)

    def find(self, **kwargs: RecordValue) -> Record | None:
        """Find a record in the index.

        This differs from :meth:`search` in that it will allow additional filtering on non-indexed columns.

        Args:
            **kwargs: The columns and values to search for.
        """
        return next(self.find_all(**kwargs), None)

    def find_all(self, **kwargs: RecordValue) -> Iterator[Record]:
        """Find all records in the index that match the given values.

        This differs from :meth:`search` in that it will allows additional filtering on non-indexed columns.
        If you only search on indexed columns, this will yield all records that match the indexed columns.

        Args:
            **kwargs: The columns and values to search for.
        """
        indexed_columns = {c.name: kwargs.pop(c.name) for c in self.index.columns}
        other_columns = kwargs

        # We need at least an exact match on the indexed columns
        self.search(**indexed_columns)

        current_key = self._primary.node().key

        # Check if we need to move the cursor back to find the first record
        while True:
            if current_key != self._primary.node().key:
                self._primary.next()
                break

            try:
                self._primary.prev()
            except NoNeighbourPageError:
                break

        while True:
            # Entries with the same indexed columns are guaranteed to be adjacent
            if current_key != self._primary.node().key:
                break

            record = self._record()
            for k, v in other_columns.items():
                value = record.get(k)
                # If the record value is a list, we do a check based on the queried value
                if isinstance(value, list):
                    # If the queried value is also a list, we check if they are equal
                    if isinstance(v, list):
                        if value != v:
                            break
                    # Otherwise we check if the queried value is in the record value
                    elif v not in value:
                        break
                else:
                    if value != v:
                        break
            else:
                yield record

            try:
                self._primary.next()
            except NoNeighbourPageError:
                break

    def record(self) -> Record:
        """Return the record the cursor is currently on.

        Returns:
            A :class:`~dissect.esedb.record.Record` object of the current record.
        """
        return self._record()

    def next(self) -> Record:
        """Move the cursor to the next record and return it.

        Can move the cursor to the next page as a side effect.

        Returns:
            A :class:`~dissect.esedb.record.Record` object of the next record.
        """
        try:
            self._primary.next()
        except NoNeighbourPageError:
            raise IndexError("No next record")
        return self._record()

    def prev(self) -> Record:
        """Move the cursor to the previous node and return it.

        Can move the cursor to the previous page as a side effect.

        Returns:
            A :class:`~dissect.esedb.record.Record` object of the previous record.
        """
        try:
            self._primary.prev()
        except NoNeighbourPageError:
            raise IndexError("No previous record")
        return self._record()
