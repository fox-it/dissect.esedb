from __future__ import annotations

import struct
from functools import cached_property
from typing import TYPE_CHECKING, Iterator, Optional, Union

from dissect.esedb.c_esedb import c_esedb, PAGE_FLAG, TAG_FLAG

if TYPE_CHECKING:
    from dissect.esedb.esedb import EseDB


class Page:
    """Represents a logical page of an ESE database.

    Args:
        esedb: An instance of :class:`~dissect.esedb.esedb.EseDB`.
        num: The logical page number.
        buf: The physical page data.
    """

    def __init__(self, esedb: EseDB, num: int, buf: bytes):
        self.esedb = esedb
        self.num = num
        self.buf = memoryview(buf)

        data_start = len(c_esedb.PGHDR)
        self.header = c_esedb.PGHDR(self.buf)
        self.header2 = None
        if not self.is_small_page:
            # Large pages have some additional header fields
            self.header2 = c_esedb.PGHDR2(self.buf[data_start:])
            data_start += len(c_esedb.PGHDR2)

        self.flags = self.header.fFlags
        self.previous_page = self.header.pgnoPrev
        self.next_page = self.header.pgnoNext

        data_end = self.header.ibMicFree
        self.data = self.buf[data_start : data_start + data_end]

        self.tag_count = self.header.itagMicFree
        self.node_count = self.tag_count - 1
        self._node_cls = LeafNode if self.is_leaf else BranchNode
        self._node_cache = {}

    @cached_property
    def is_small_page(self) -> bool:
        return self.esedb.has_small_pages

    @cached_property
    def is_root(self) -> bool:
        return bool(self.flags & PAGE_FLAG.Root)

    @cached_property
    def is_leaf(self) -> bool:
        return bool(self.flags & PAGE_FLAG.Leaf)

    @cached_property
    def is_parent(self) -> bool:
        return bool(self.flags & PAGE_FLAG.ParentOfLeaf)

    @cached_property
    def is_empty(self) -> bool:
        return bool(self.flags & PAGE_FLAG.Empty)

    @cached_property
    def is_space_tree(self) -> bool:
        return bool(self.flags & PAGE_FLAG.SpaceTree)

    @cached_property
    def is_index(self) -> bool:
        return bool(self.flags & PAGE_FLAG.Index)

    @cached_property
    def is_long_value(self) -> bool:
        return bool(self.flags & PAGE_FLAG.LongValue)

    @cached_property
    def is_branch(self) -> bool:
        return not self.is_leaf

    @cached_property
    def key_prefix(self) -> Optional[bytes]:
        if not self.is_root:
            return bytes(self.tag(0).data)

    def tag(self, num: int) -> Tag:
        """Retrieve a tag by index.

        Args:
            num: The tag number to retrieve.

        Raises:
            IndexError: If the tag number is out of bounds.
        """
        if num < 0 or num > self.tag_count - 1:
            raise IndexError(f"Tag number exceeds boundaries: 0-{self.tag_count - 1}")

        return Tag(self, num)

    def tags(self) -> Iterator[Tag]:
        """Yield all tags."""
        for i in range(1, self.tag_count):
            yield self.tag(i)

    def node(self, num: int) -> Union[BranchNode, LeafNode]:
        """Retrieve a node by index.

        Nodes are just tags, but indexed from the first tag.

        Args:
            num: The node number to retrieve.

        Raises:
            IndexError: If the node number is out of bounds.
        """
        if num < 0 or num > self.node_count - 1:
            raise IndexError(f"Node number exceeds boundaries: 0-{self.node_count - 1}")

        if num not in self._node_cache:
            self._node_cache[num] = self._node_cls(self.tag(num + 1))

        return self._node_cache[num]

    def nodes(self) -> Iterator[Union[BranchNode, LeafNode]]:
        """Yield all nodes."""
        for i in range(self.node_count):
            yield self.node(i)

    def iter_leaf_nodes(self) -> Iterator[LeafNode]:
        """Walk the page tree and yield leaf nodes.

        Two methods can be used, one is to travel down to the first leaf, and keep reading ``next_page``'s,
        the other is to traverse the tree branches.

        Impacket uses the first method, but gets caught in an infinite loop on some dirty databases.
        Traversing the branches seems safer, at the risk of missing a couple (possibly corrupt) pages.

        For this reason, we actually explicitly check if the last page we parse has a ``next_page`` attribute,
        and also parse that. This methods seems to work so far.
        """
        esedb = self.esedb
        leaf = None

        for node in self.nodes():
            if self.is_leaf:
                yield node
            else:
                child = esedb.page(node.child)
                for leaf in child.iter_leaf_nodes():
                    yield leaf

        if self.is_root and leaf and leaf.tag.page.next_page:
            for leaf in esedb.page(leaf.tag.page.next_page).iter_leaf_nodes():
                yield leaf

    def __repr__(self) -> str:
        return f"<Page num={self.num:d}>"


class Tag:
    """A tag is the "physical" data entry of a page.

    Args:
        page: The :class:`Page` this tag is in.
        num: The tag number to parse.
    """

    __slots__ = ("page", "num", "tag", "offset", "size", "data", "flags")

    def __init__(self, page: Page, num: int):
        self.page = page
        self.num = num

        tag_offset = len(page.buf) + ((num + 1) * -4)
        tag_buf = page.buf[tag_offset : tag_offset + 4]

        self.tag = c_esedb.TAG(tag_buf)

        mask = 0x1FFF if page.is_small_page else 0x7FFF
        self.size = self.tag.cb_ & mask
        self.offset = self.tag.ib_ & mask

        self.data = self.page.data[self.offset : self.offset + self.size]

        flags = 0
        if page.is_small_page:
            # Small pages have the flag in the tag
            flags = self.tag.ib_ >> 13
        elif len(self.data) >= 2:
            # Large pages have the flag in the first USHORT
            # Also in the 3 MSB, just like the small page, but we know it'll be little endian so do a shortcut on
            # the second byte.
            flags = self.data[1] >> 5

        self.flags = TAG_FLAG(flags)

    def __repr__(self) -> str:
        return f"<Tag offset=0x{self.offset:x} size=0x{self.size:x}>"


class Node:
    """A node is the "logical" data entry of a page.

    Args:
        tag: The :class:`Tag` to parse a node from.
    """

    __slots__ = ("tag", "num", "key", "key_prefix", "key_suffix", "data")

    def __init__(self, tag: Tag):
        self.tag = tag
        self.num = tag.num - 1

        buf = tag.data
        offset = 0

        key_prefix = b""
        key_prefix_size = None

        # Large pages have the tag flags encoded in the 3 MSB of the first word, so we have to mask the first 13 bits
        # See also the Tag class
        if len(buf) >= offset + 2 and tag.flags & TAG_FLAG.Compressed:
            key_prefix_size = struct.unpack("<H", buf[:2])[0] & 0x1FFF
            key_prefix = self.tag.page.key_prefix[:key_prefix_size].ljust(key_prefix_size, b"\x00")
            offset += 2

        key_suffix = b""
        key_suffix_size = None

        if len(buf) >= offset + 2:
            key_suffix_size = struct.unpack("<H", buf[offset : offset + 2])[0] & 0x1FFF
            offset += 2
            key_suffix = buf[offset : offset + key_suffix_size]
            offset += key_suffix_size

        self.key = key_prefix + key_suffix
        self.key_prefix = key_prefix
        self.key_suffix = key_suffix
        self.data = buf[offset:]


class LeafNode(Node):
    """Special leaf node."""

    def __repr__(self) -> str:
        return f"<LeafNode key={self.key}>"


class BranchNode(Node):
    """Special branch node. Parses the child page information."""

    __slots__ = ("child",)

    def __init__(self, tag: Tag):
        super().__init__(tag)
        self.child = struct.unpack("<I", self.data[:4])[0]

    def __repr__(self) -> str:
        return f"<BranchNode key={self.key} child={self.child}>"
