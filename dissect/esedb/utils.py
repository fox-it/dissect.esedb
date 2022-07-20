""" Utils for esedb module """
from collections import namedtuple

from dissect import cstruct

# https://github.com/libyal/libesedb
# https://github.com/SecureAuthCorp/impacket/blob/master/impacket/ese.py

c_esedb_def = """

typedef struct GUID {
    char value[16];  // typedef of an array is still broken
};


flag PAGE_FLAG : uint32 {
    ROOT            = 0x01,
    LEAF            = 0x02,
    PARENT          = 0x04,
    EMPTY           = 0x08,
    SPACE_TREE      = 0x20,
    INDEX           = 0x40,
    LONG_VALUE      = 0x80,
    NEW_FORMAT      = 0x2000,
    SCRUBBED        = 0x4000
};

flag PAGE_TAG_FLAG : uint16 {
    UNKNOWN         = 0x01,
    DEFUNCT         = 0x02,
    COMMON          = 0x04
};

flag TAGGED_DATA_TYPE : uint8 {
    VARIABLE_SIZE           = 0x01,
    COMPRESSED              = 0x02,
    STORED                  = 0x04,
    MULTI_VALUE             = 0x08,
    MULTI_VALUE_SIZE_DEF    = 0x10
};

enum COLUMN_TYPE {
    JET_coltypNil           = 0,
    JET_coltypBit           = 1,
    JET_coltypUnsignedByte  = 2,
    JET_coltypShort         = 3,
    JET_coltypLong          = 4,
    JET_coltypCurrency      = 5,
    JET_coltypIEEESingle    = 6,
    JET_coltypIEEEDouble    = 7,
    JET_coltypDateTime      = 8,
    JET_coltypBinary        = 9,
    JET_coltypText          = 10,
    JET_coltypLongBinary    = 11,
    JET_coltypLongText      = 12,
    JET_coltypSLV           = 13,
    JET_coltypUnsignedLong  = 14,
    JET_coltypLongLong      = 15,
    JET_coltypGUID          = 16,
    JET_coltypUnsignedShort = 17,
    JET_coltypMax           = 18
};

struct esedb_file_header {
    uint32      checksum;
    char        signature[4];
    uint32      format_version;
    uint32      file_type;
    uint64      database_time;
    char        database_signature[28];
    uint32      database_state;
    uint64      consistent_position;
    uint64      consistent_time;
    uint64      attach_time;
    uint64      attach_position;
    uint64      detach_time;
    uint64      detach_position;
    char        log_signature[28];
    uint32      unknown;
    char        previous_full_backup[24];
    char        previous_incremental_backup[24];
    char        current_full_backup[24];
    uint32      shadowing_disabled;
    uint32      last_object_identifier;
    uint32      index_update_major_version;
    uint32      index_update_minor_version;
    uint32      index_update_build_number;
    uint32      index_update_service_pack_number;
    uint32      format_revision;
    uint32      page_size;
    uint32      repair_count;
    uint64      repair_time;
    char        unknown2[28];
    uint64      scrub_database_time;
    uint64      scrub_time;
    uint64      required_log;
    uint32      upgrade_exchange5_format;
    uint32      upgrade_free_pages;
    uint32      upgrade_space_map_pages;
    char        current_shadow_volume_backup[24];
    uint32      creation_format_version;
    uint32      creation_format_revision;
    char        unknown3[16];
    uint32      old_repair_count;
    uint32      ecc_fix_succes_count;
    uint64      ecc_fix_succes_time;
    uint32      old_ecc_fix_succes_count;
    uint32      ecc_fix_error_count;
    uint64      ecc_fix_error_time;
    uint32      old_ecc_fix_error_count;
    uint32      bad_checksum_error_count;
    uint64      bad_checksum_error_time;
    uint32      old_bac_checksum_error_count;
    uint32      committed_log;
    char        previous_shadow_volume_backup[24];
    char        previous_differential_backup[24];
    char        unknown4[40];
    uint32      nls_major_version;
    uint32      nls_minor_version;
    char        unknown5[148];
    uint32      unknown_flags;
};

struct page_common {
    uint64      data_modification_time;
    uint32      previous_page;
    uint32      next_page;
    uint32      father_data_page;
    uint16      available_data_size;
    uint16      available_uncommitted_data_size;
    uint16      first_available_data_offset;
    uint16      first_available_page_tag;
    PAGE_FLAG   flags;
};

struct page_xp_sp0 {
    uint32      checksum;
    uint32      page_number;
    page_common page_common;
};

struct page_xp_sp1 {
    uint32      checksum;
    uint32      ecc_checksum;
    page_common page_common;
};

struct page_win7 {
    uint64      checksum;
    page_common page_common;
};

struct page_win7_extended {
    uint64      checksum;
    page_common page_common;
    uint64      extended_checksum1;
    uint64      extended_checksum2;
    uint64      extended_checksum3;
    uint64      page_number;
    uint64      unknown;
};

struct page_tag_xp {
    uint16          value_size:13;
    uint16          unknown:3;
    uint16          value_offset:13;
    PAGE_TAG_FLAG   flags:3;
};

struct page_tag_win7 {
    uint16      value_size:15;
    uint16      unk0:1;
    uint16      value_offset:15;
    uint16      unk1:1;
};

struct root_page_header {
    uint32      initial_number_of_pages;
    uint32      parent_fdp;
    uint32      extent_space;
    uint32      space_tree_page_number;
};

struct space_tree_header {
    char        unk0[16];
};

struct space_tree_entry {
    uint16      size;
    char        page_key[size];
    uint32      page_count;
};

struct leaf_page_entry_common {
    uint16      common_page_key_size;
    uint16      local_page_key_size;
    char        local_page_key[local_page_key_size];
};

struct leaf_page_entry {
    uint16      local_page_key_size;
    char        local_page_key[local_page_key_size];
};

struct branch_page_entry_common {
    uint16      common_page_key_size;
    uint16      local_page_key_size;
    char        local_page_key[local_page_key_size];
    uint32      child_page_number;
};

struct branch_page_entry {
    uint16      local_page_key_size;
    char        local_page_key[local_page_key_size];
    uint32      child_page_number;
};

struct data_definition_header {
    uint8       last_fixed_id;
    uint8       last_variable_id;
    uint16      variable_data_offset;
};
"""

c_esedb = cstruct.cstruct()
c_esedb.load(c_esedb_def)

PAGE_FLAG = c_esedb.PAGE_FLAG
PAGE_TAG_FLAG = c_esedb.PAGE_TAG_FLAG
COLUMN_TYPE = c_esedb.COLUMN_TYPE
TAGGED_DATA_TYPE = c_esedb.TAGGED_DATA_TYPE

DATABASE_PAGE_NUMBER = 1
CATALOG_PAGE_NUMBER = 4
CATALOG_BACKUP_PAGE_NUMBER = 24

DATA_DEFINITION_HEADER_SIZE = 4

TAGGED_DATA_FORMAT_INDEX = 1
TAGGED_DATA_FORMAT_LINEAR = 2

CODEPAGE_UNICODE = 1200
CODEPAGE_WESTERN = 1252
CODEPAGE_ASCII = 20127

CODEPAGE_MAP = {
    CODEPAGE_UNICODE: "utf-16-le",
    CODEPAGE_WESTERN: "cp1252",
    CODEPAGE_ASCII: "ascii",
}


def decode_text(buf, encoding):
    codec = CODEPAGE_MAP[encoding]
    return buf.decode(codec)


ColumnType = namedtuple("ColumnType", ["value", "name", "parse"])

COLUMN_TYPES = [
    ColumnType(COLUMN_TYPE.JET_coltypNil, "NULL", None),
    ColumnType(COLUMN_TYPE.JET_coltypBit, "Boolean", c_esedb.uint8),
    ColumnType(COLUMN_TYPE.JET_coltypUnsignedByte, "Signed byte", c_esedb.uint8),
    ColumnType(COLUMN_TYPE.JET_coltypShort, "Signed short", c_esedb.int16),
    ColumnType(COLUMN_TYPE.JET_coltypLong, "Signed long", c_esedb.int32),
    ColumnType(COLUMN_TYPE.JET_coltypCurrency, "Currency", c_esedb.uint64),
    ColumnType(COLUMN_TYPE.JET_coltypIEEESingle, "Single precision FP", c_esedb.float),
    ColumnType(COLUMN_TYPE.JET_coltypIEEEDouble, "Double precision FP", c_esedb.double),
    ColumnType(COLUMN_TYPE.JET_coltypDateTime, "DateTime", c_esedb.uint64),
    ColumnType(COLUMN_TYPE.JET_coltypBinary, "Binary", None),
    ColumnType(COLUMN_TYPE.JET_coltypText, "Text", decode_text),
    ColumnType(COLUMN_TYPE.JET_coltypLongBinary, "Long Binary", None),
    ColumnType(COLUMN_TYPE.JET_coltypLongText, "Long Text", decode_text),
    ColumnType(COLUMN_TYPE.JET_coltypSLV, "Super Long Value", None),
    ColumnType(COLUMN_TYPE.JET_coltypUnsignedLong, "Unsigned long", c_esedb.uint32),
    ColumnType(COLUMN_TYPE.JET_coltypLongLong, "Long long", c_esedb.uint64),
    ColumnType(COLUMN_TYPE.JET_coltypGUID, "GUID", c_esedb.GUID),
    ColumnType(COLUMN_TYPE.JET_coltypUnsignedShort, "Unsigned short", c_esedb.uint16),
    ColumnType(COLUMN_TYPE.JET_coltypMax, "Max", None),
]

COLUMN_TYPE_MAP = {t.value.value: t for t in COLUMN_TYPES}


def noop(value):
    return value
