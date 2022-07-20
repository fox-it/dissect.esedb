import io

from dissect.esedb.utils import c_esedb, PAGE_FLAG, PAGE_TAG_FLAG
from dissect.esedb.exceptions import InvalidTagNumber


class Page:
    def __init__(self, esedb, num, buf):
        self.esedb = esedb
        self.num = num
        self.buf = memoryview(buf)

        self._header_struct = None
        self._tag_struct = c_esedb.page_tag_xp

        if esedb.format_version < 0x620 or esedb.format_version and esedb.format_revision < 0x0B:
            self._header_struct = c_esedb.page_xp_sp0

        elif esedb.format_version == 0x620 and esedb.format_revision < 0x11:
            self._header_struct = c_esedb.page_xp_sp1

        else:
            if esedb.header.page_size > 8192:
                self._tag_struct = c_esedb.page_tag_win7
                self._header_struct = c_esedb.page_win7_extended
            else:
                self._header_struct = c_esedb.page_win7

        self.header = self._header_struct(buf)
        self.flags = self.header.page_common.flags

        self.previous_page = self.header.page_common.previous_page
        self.next_page = self.header.page_common.next_page

        data_start = len(self._header_struct)
        data_end = self.header.page_common.first_available_data_offset
        self.data = self.buf[data_start : data_start + data_end]

        self.tag_count = self.header.page_common.first_available_page_tag
        self.node_count = self.tag_count - 1
        self._node_cls = LeafNode if self.is_leaf else BranchNode
        self._node_cache = {}

    @property
    def is_root(self):
        return bool(self.flags & PAGE_FLAG.ROOT)

    @property
    def is_leaf(self):
        return bool(self.flags & PAGE_FLAG.LEAF)

    @property
    def is_parent(self):
        return bool(self.flags & PAGE_FLAG.PARENT)

    @property
    def is_empty(self):
        return bool(self.flags & PAGE_FLAG.EMPTY)

    @property
    def is_space_tree(self):
        return bool(self.flags & PAGE_FLAG.SPACE_TREE)

    @property
    def is_index(self):
        return bool(self.flags & PAGE_FLAG.INDEX)

    @property
    def is_long_value(self):
        return bool(self.flags & PAGE_FLAG.LONG_VALUE)

    @property
    def is_branch(self):
        return not self.is_leaf

    @property
    def common_key(self):
        if not self.is_root:
            # self.tag(0).value could be a memoryview or a bytestring,
            # always return a bytestring
            return bytes(self.tag(0).value)

    def tag(self, num):
        """Get a tag by index."""
        if num < 0 or num > self.tag_count:
            raise InvalidTagNumber("Tag number exceeds boundaries")

        return Tag(self, num)

    def tags(self):
        """Yield all tags."""
        for i in range(1, self.tag_count):
            yield self.tag(i)

    def node(self, num):
        """Get a node by index.

        Nodes are just tags, but indexed from the first tag.
        """
        if num not in self._node_cache:
            self._node_cache[num] = self._node_cls(self.tag(num + 1))

        return self._node_cache[num]

    def nodes(self):
        """Yield all nodes."""
        for i in range(self.node_count):
            yield self.node(i)

    def find_key(self, key, common_key=None):
        """Find a key in the B+-tree.

        The EseDB B+-tree is weird, so this is half actual B+-tree traversing
        and half bruteforce.

        Should probably just implement a proper B+-tree at some point...
        """
        boundless_node = None

        for node in self.nodes():
            page_key = node.page_key

            check_len = min(len(key), len(page_key))
            lkey = key[:check_len]
            lpage_key = page_key[:check_len]

            if (
                self.is_leaf
                and (not common_key or not len(common_key))
                and not len(node.local_key)
                and len(key) == len(page_key)
            ):
                boundless_node = node
                continue
            elif check_len and lkey > lpage_key:
                continue

            if self.is_branch:
                if check_len == 0:
                    try:
                        return self.esedb.page(node.child).find_key(key, page_key)
                    except ValueError:
                        pass

                if self.is_long_value:
                    if lkey <= lpage_key:
                        try:
                            return self.esedb.page(node.child).find_key(key, page_key)
                        except ValueError:
                            pass
            elif self.is_leaf:
                if len(key) != len(page_key):
                    continue

                if lkey < lpage_key:
                    return node
                elif len(key) < len(page_key):
                    return node
                else:
                    return node

        if self.is_leaf and boundless_node:
            return boundless_node

        raise ValueError("not found")

    def walk(self):
        """Walk the page tree and yield leaf nodes.

        Two methods can be used, one is to travel down to the
        first leaf, and keep reading next_page's, the other is
        to traverse the tree branches. Impacket uses the first
        method, but gets caught in an infinite loop on some
        dirty databases. Traversing the branches seems safer,
        at the risk of missing a couple pages.
        For this reason, we actually explicitly check if the
        last page we parse has a next_page attribute, and also
        parse that. This methods seems to work so far.
        """
        esedb = self.esedb
        leaf = None

        for node in self.nodes():
            if self.is_leaf:
                yield node
            else:
                child = esedb.page(node.child)
                for leaf in child.walk():
                    yield leaf

        if self.is_root and leaf and leaf.tag.page.next_page:
            for leaf in esedb.page(leaf.tag.page.next_page).walk():
                yield leaf

    def __repr__(self):
        return "<Page num={}>".format(self.num)


class RootPage:
    def __init__(self, page):
        self.page = page
        self.header = c_esedb.root_page_header(page.tag(0).value)


# region nodes
class Tag:
    __slots__ = ("page", "num", "tag", "offset", "size", "value", "flags")

    def __init__(self, page, num):
        self.page = page
        self.num = num

        tag_offset = len(page.buf) + ((num + 1) * -4)
        tag_buf = page.buf[tag_offset : tag_offset + 4]

        tag_struct = page._tag_struct
        self.tag = tag_struct(tag_buf)

        self.offset = self.tag.value_offset
        self.size = self.tag.value_size

        self.value = self.page.data[self.offset : self.offset + self.size]
        # Note: self.value could be a memoryview type instance or a bytestring
        # (if it has been editted).
        if tag_struct == c_esedb.page_tag_win7:
            # self.value is a bytestring
            value = bytearray(self.value)
            self.flags = PAGE_TAG_FLAG((value[1] & 0xE0) >> 5)
            value[1] &= 0x1F
            self.value = bytes(value)
        else:
            # self.value is a memoryview
            self.flags = self.tag.flags

    def __repr__(self):
        return "<Tag offset=0x{:x} size=0x{:x}>".format(self.offset, self.size)


class Node:
    __slots__ = (
        "tag",
        "page",
        "node",
        "child",
        "value",
        "local_key",
        "local_key_size",
        "common_key",
        "common_key_size",
    )

    def __init__(self, tag):
        self.tag = tag
        self.page = tag.page
        self.node = None
        self.child = None
        self.value = None

        self.local_key = None
        self.local_key_size = None
        self.common_key_size = None

    @property
    def page_key(self):
        if self.common_key_size and self.common_key_size > 0:
            common_key = self.page.common_key[: self.common_key_size].ljust(self.common_key_size, b"\x00")
        else:
            common_key = b""
        return common_key + self.local_key

    def debug(self):
        page_offset = len(self.tag.page._header_struct) + self.tag.offset
        # TAG   1: cb:0x0009,ib:0x064c prefix:cb=0x0000 suffix:cb=0x0003
        #  data:cb=0x0004 offset:0x0674-0x067d flags:0x0000 (   )    pgno: 989 (0x3dd)
        res = (
            "TAG {:3d}: "
            "cb:0x{:04x},ib:0x{:04x} "
            "prefix:cb=0x{:04x} "
            "suffix:cb=0x{:04x} "
            "data:cb=0x{:04x} "
            "offset:0x{:04x}-0x{:04x} "
            "flags:0x{:04x} (   )"
        ).format(
            self.tag.num,
            self.tag.size,
            self.tag.offset,
            self.common_key_size or 0,
            self.local_key_size or 0,
            len(self.value),
            page_offset,
            page_offset + self.tag.size,
            self.tag.flags,
        )

        if self.child:
            res += "    pgno: {0} (0x{0:x})".format(self.child)

        return res


class LeafNode(Node):
    def __init__(self, tag):
        super().__init__(tag)
        buf = io.BytesIO(tag.value)
        if tag.flags & PAGE_TAG_FLAG.COMMON:
            self.node = c_esedb.leaf_page_entry_common(buf)
            self.common_key_size = self.node.common_page_key_size
            common_key = self.page.common_key[: self.common_key_size]
        else:
            self.node = c_esedb.leaf_page_entry(buf)
            common_key = b""

        self.value = buf.read()

        self.local_key = self.node.local_page_key
        self.local_key_size = self.node.local_page_key_size
        self.key = common_key + self.local_key

    def __repr__(self):
        return "<LeafNode local_key={}>".format(self.local_key)


class BranchNode(Node):
    def __init__(self, tag):
        super().__init__(tag)
        if tag.flags & PAGE_TAG_FLAG.COMMON:
            self.node = c_esedb.branch_page_entry_common(tag.value)
            self.common_key_size = self.node.common_page_key_size
            common_key = self.page.common_key[: self.common_key_size]
        else:
            self.node = c_esedb.branch_page_entry(tag.value)
            common_key = b""

        self.child = self.node.child_page_number

        self.local_key = self.node.local_page_key
        self.local_key_size = self.node.local_page_key_size
        self.key = common_key + self.local_key

    def __repr__(self):
        return "<BranchNode local_key={} child={}>".format(self.local_key, self.child)


# endregion
