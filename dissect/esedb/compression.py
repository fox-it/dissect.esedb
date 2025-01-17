from __future__ import annotations

import struct

from dissect.util.compression import lzxpress, sevenbit

from dissect.esedb.c_esedb import COMPRESSION_SCHEME


def decompress(buf: bytes) -> bytes:
    """Decompress the given bytes according to the encoded compression scheme.

    Args:
        buf: The compressed bytes to decompress.

    Raises:
        NotImplementedError: If the buffer is compressed with an unsupported compression algorithm (XPRESS9/XPRESS10).
    """
    identifier = buf[0] >> 3
    if identifier == COMPRESSION_SCHEME.COMPRESS_7BITASCII:
        return sevenbit.decompress(buf[1:])
    if identifier == COMPRESSION_SCHEME.COMPRESS_7BITUNICODE:
        return sevenbit.decompress(buf[1:], wide=True)
    if identifier == COMPRESSION_SCHEME.COMPRESS_XPRESS:
        return lzxpress.decompress(buf[3:])
    if identifier in (COMPRESSION_SCHEME.COMPRESS_XPRESS9, COMPRESSION_SCHEME.COMPRESS_XPRESS10):
        raise NotImplementedError(f"Compression not yet implemented: {COMPRESSION_SCHEME(identifier)}")
    # Not compressed
    return buf


def decompress_size(buf: bytes) -> int | None:
    """Return the decompressed size of the given bytes according to the encoded compression scheme.

    Args:
        buf: The compressed bytes to return the decompressed size of.

    Raises:
        NotImplementedError: If the buffer is compressed with an unsupported compression algorithm (XPRESS9/XPRESS10).
    """
    identifier = buf[0] >> 3
    if identifier == COMPRESSION_SCHEME.COMPRESS_7BITASCII:
        return ((buf[0] & 7) + (8 * len(buf))) // 7
    if identifier == COMPRESSION_SCHEME.COMPRESS_7BITUNICODE:
        return 2 * (((buf[0] & 7) + (8 * len(buf))) // 7)
    if identifier == COMPRESSION_SCHEME.COMPRESS_XPRESS:
        return struct.unpack("<H", buf[1:2])[0]
    if identifier in (COMPRESSION_SCHEME.COMPRESS_XPRESS9, COMPRESSION_SCHEME.COMPRESS_XPRESS10):
        raise NotImplementedError(f"Compression not yet implemented: {COMPRESSION_SCHEME(identifier)}")
    return None
