from __future__ import annotations

from typing import TYPE_CHECKING, Union

from dissect.esedb.page import Page, Node
from dissect.esedb.exceptions import KeyNotFoundError, NoNeighbourPageError

if TYPE_CHECKING:
    from dissect.esedb.esedb import EseDB


class Cursor:
    """A simple cursor implementation for searching the ESE B+Trees

    Args:
        esedb: An instance of :class:`~dissect.esedb.esedb.EseDB`.
        page: The page to open a cursor on.
    """

    def __init__(self, esedb: EseDB, page: Union[int, Page]):
        self.esedb = esedb

        if isinstance(page, int):
            page_num = page
            page = esedb.page(page_num)
        else:
            page_num = page.num

        self._page = page
        self._page_num = page_num
        self._node_num = 0

    def node(self) -> Node:
        """Return the node the cursor is currently on."""
        return self._page.node(self._node_num)

    def next(self) -> Node:
        """Move the cursor to the next node and return it.

        Can move the cursor to the next page as a side effect.
        """
        if self._node_num + 1 > self._page.node_count - 1:
            self.next_page()
        else:
            self._node_num += 1

        return self.node()

    def next_page(self) -> None:
        """Move the cursor to the next page in the tree.

        Raises:
            NoNeighbourPageError: If the current page has no next page.
        """
        if self._page.next_page:
            self._page = self.esedb.page(self._page.next_page)
            self._node_num = 0
        else:
            raise NoNeighbourPageError(f"{self._page} has no next page")

    def prev(self) -> Node:
        """Move the cursor to the previous node and return it.

        Can move the cursor to the previous page as a side effect.
        """
        if self._node_num - 1 < 0:
            self.prev_page()
        else:
            self._node_num -= 1

        return self.node()

    def prev_page(self) -> None:
        """Move the cursor to the previous page in the tree.

        Raises:
            NoNeighbourPageError: If the current page has no previous page.
        """
        if self._page.previous_page:
            self._page = self.esedb.page(self._page.previous_page)
            self._node_num = self._page.node_count - 1
        else:
            raise NoNeighbourPageError(f"{self._page} has no previous page")

    def search(self, key: bytes, exact: bool = True) -> Node:
        """Search the tree for the given key.

        Moves the cursor to the matching node, or on the last node that is less than the requested key.

        Args:
            key: The key to search for.
            exact: Whether to only return successfully on an exact match.

        Raises:
            KeyNotFoundError: If an ``exact`` match was requested but not found.
        """
        page = self._page
        while True:
            node = find_node(page, key)

            if page.is_branch:
                page = self.esedb.page(node.child)
            else:
                self._page = page
                self._page_num = page.num
                self._node_num = node.num
                break

        if exact and key != node.key:
            raise KeyNotFoundError(f"Can't find key: {key}")

        return self.node()


def find_node(page: Page, key: bytes) -> Node:
    """Search the tree, starting from the given ``page`` and search for ``key``.

    Args:
        page: The page to start searching from. Should be a branch page.
        key: The key to search.
    """
    first_node_idx = 0
    last_node_idx = page.node_count - 1

    node = None
    while first_node_idx < last_node_idx:
        node_idx = (first_node_idx + last_node_idx) // 2
        node = page.node(node_idx)

        # It turns out that the way BTree keys are compared matches 1:1 with how Python compares bytes
        # First compare data, then length
        if key < node.key:
            last_node_idx = node_idx
        elif key == node.key:
            if page.is_branch:
                # If there's an exact match on a key on a branch page, the actual leaf nodes are in the next branch
                # Page keys for branch pages appear to be non-inclusive upper bounds
                node_idx = min(node_idx + 1, page.node_count - 1)
                node = page.node(node_idx)

            return node
        else:
            first_node_idx = node_idx + 1

    # We're at the last node
    return page.node(first_node_idx)
