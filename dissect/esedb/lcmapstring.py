# Based on Wine source
# https://github.com/wine-mirror/wine/blob/master/dlls/kernelbase/locale.c

from enum import IntEnum, IntFlag

from dissect.util.xmemoryview import xmemoryview

from dissect.esedb.sorting_table import table


class MapFlags(IntFlag):
    NORM_IGNORECASE = 0x00000001  # ignore case
    NORM_IGNORENONSPACE = 0x00000002  # ignore nonspacing chars
    NORM_IGNORESYMBOLS = 0x00000004  # ignore symbols

    LINGUISTIC_IGNORECASE = 0x00000010  # linguistically appropriate 'ignore case'
    LINGUISTIC_IGNOREDIACRITIC = 0x00000020  # linguistically appropriate 'ignore nonspace'

    NORM_IGNOREKANATYPE = 0x00010000  # ignore kanatype
    NORM_IGNOREWIDTH = 0x00020000  # ignore width
    NORM_LINGUISTIC_CASING = 0x08000000  # use linguistic rules for casing

    LCMAP_LOWERCASE = 0x00000100  # lower case letters
    LCMAP_UPPERCASE = 0x00000200  # UPPER CASE LETTERS
    LCMAP_TITLECASE = 0x00000300  # Title Case Letters

    LCMAP_SORTKEY = 0x00000400  # WC sort key (normalize)
    LCMAP_BYTEREV = 0x00000800  # byte reversal

    LCMAP_HIRAGANA = 0x00100000  # map katakana to hiragana
    LCMAP_KATAKANA = 0x00200000  # map hiragana to katakana
    LCMAP_HALFWIDTH = 0x00400000  # map double byte to single byte
    LCMAP_FULLWIDTH = 0x00800000  # map single byte to double byte

    LCMAP_LINGUISTIC_CASING = 0x01000000  # use linguistic rules for casing

    LCMAP_SIMPLIFIED_CHINESE = 0x02000000  # map traditional chinese to simplified chinese
    LCMAP_TRADITIONAL_CHINESE = 0x04000000  # map simplified chinese to traditional chinese

    LCMAP_SORTHANDLE = 0x20000000
    LCMAP_HASH = 0x00040000

    SORT_STRINGSORT = 0x00001000  # use string sort method
    SORT_DIGITSASNUMBERS = 0x00000008  # use digits as numbers sort method


class SCRIPT(IntEnum):
    UNSORTABLE = 0
    NONSPACE_MARK = 1
    EXPANSION = 2
    EASTASIA_SPECIAL = 3
    JAMO_SPECIAL = 4
    EXTENSION_A = 5
    PUNCTUATION = 6
    SYMBOL_1 = 7
    SYMBOL_2 = 8
    SYMBOL_3 = 9
    SYMBOL_4 = 10
    SYMBOL_5 = 11
    SYMBOL_6 = 12
    DIGIT = 13
    LATIN = 14
    GREEK = 15
    CYRILLIC = 16
    KANA = 34
    HEBREW = 40
    ARABIC = 41
    PUA_FIRST = 169
    PUA_LAST = 175
    CJK_FIRST = 192
    CJK_LAST = 239


class CASE(IntFlag):
    FULLWIDTH = 0x01  # full width kana (vs. half width)
    FULLSIZE = 0x02  # full size kana (vs. small)
    SUBSCRIPT = 0x08  # sub/super script
    UPPER = 0x10  # upper case
    KATAKANA = 0x20  # katakana (vs. hiragana)
    COMPR_2 = 0x40  # compression exists for >= 2 chars
    COMPR_4 = 0x80  # compression exists for >= 4 chars
    COMPR_6 = 0xC0  # compression exists for >= 6 chars


def map_string(value: str, flags: MapFlags, locale: str) -> bytes:
    """Very basic Python implementation of LCMapStringEx, only supporting sorting keys.

    Currently only supports one hardcoded sorting table (the default) and the basic character types.

    Args:
        value: The string to map to a sorting key.
        flags: The flags passed to LCMapStringEx
        locale: The locale to use for mapping.

    Returns:
        A sorting key of the given input that should be compatible with LCMapStringEx.

    Raises:
        NotImplementedError: If an unsupported flag or character is encountered.
    """
    key_primary = []
    key_diacritic = []
    key_case = []

    if not flags & MapFlags.LCMAP_SORTKEY:
        raise NotImplementedError("Only LCMAP_SORTKEY is partially supported")

    case_mask = 0x3F
    if flags & MapFlags.NORM_IGNORECASE:
        case_mask &= ~(CASE.UPPER | CASE.SUBSCRIPT)
    if flags & MapFlags.NORM_IGNOREWIDTH:
        case_mask &= ~CASE.FULLWIDTH
    if flags & MapFlags.NORM_IGNOREKANATYPE:
        case_mask &= ~CASE.KATAKANA

    view = xmemoryview(value.encode("utf-16-le"), "<H")
    for cp in view:
        weight = table[cp]

        alphabetic_weight = weight >> 24
        script_member = (weight >> 16) & 0xFF
        diacritic_weight = (weight >> 8) & 0xFF
        case_weight = (weight & 0xFF) & case_mask

        if script_member == SCRIPT.UNSORTABLE:
            continue

        if script_member == SCRIPT.NONSPACE_MARK:
            if flags & MapFlags.LINGUISTIC_IGNOREDIACRITIC:
                diacritic_weight = 2

            if len(key_diacritic):
                key_diacritic[-1] += diacritic_weight
            else:
                key_diacritic.append(diacritic_weight)

        if script_member == SCRIPT.EXPANSION:
            raise NotImplementedError(SCRIPT.EXPANSION)

        if script_member == SCRIPT.EASTASIA_SPECIAL:
            raise NotImplementedError(SCRIPT.EASTASIA_SPECIAL)

        if script_member == SCRIPT.JAMO_SPECIAL:
            raise NotImplementedError(SCRIPT.JAMO_SPECIAL)

        if script_member == SCRIPT.EXTENSION_A:
            key_primary.append(0xFD)
            key_primary.append(0xFF)
            key_primary.append(alphabetic_weight)
            key_primary.append(diacritic_weight)
            key_diacritic.append(2)
            key_case.append(2)
            continue

        if script_member == SCRIPT.PUNCTUATION:
            if flags & MapFlags.NORM_IGNORESYMBOLS:
                continue

            if not flags & MapFlags.SORT_STRINGSORT:
                raise NotImplementedError(SCRIPT.PUNCTUATION)

            key_primary.append(script_member)
            key_primary.append(alphabetic_weight)
            key_diacritic.append(diacritic_weight)
            key_case.append(case_weight)
            continue

        if script_member in (
            SCRIPT.SYMBOL_1,
            SCRIPT.SYMBOL_2,
            SCRIPT.SYMBOL_3,
            SCRIPT.SYMBOL_4,
            SCRIPT.SYMBOL_5,
            SCRIPT.SYMBOL_6,
        ):
            if flags & MapFlags.NORM_IGNORESYMBOLS:
                continue
            key_primary.append(script_member)
            key_primary.append(alphabetic_weight)
            key_diacritic.append(diacritic_weight)
            key_case.append(case_weight)
            continue

        if script_member == SCRIPT.DIGIT and 0:
            raise NotImplementedError(SCRIPT.DIGIT)

        # else
        key_primary.append(script_member)
        key_primary.append(alphabetic_weight)
        key_diacritic.append(diacritic_weight)
        key_case.append(case_weight)

    key_diacritic = _filter_weights(key_diacritic)
    if flags & (MapFlags.NORM_IGNORECASE | MapFlags.NORM_IGNOREWIDTH):
        key_case = []
    else:
        key_case = _filter_weights(key_case)

    return bytes(
        [
            *key_primary,
            0x01,
            *(key_diacritic if not flags & MapFlags.NORM_IGNORENONSPACE else []),
            0x01,
            *key_case,
            0x01,
            # extra would go here
            0x01,
            # special would go here
            0x00,
        ]
    )


def _filter_weights(weights):
    i = len(weights)
    while i > 0:
        if weights[i - 1] > 2:
            break
        i -= 1
    return weights[:i]
