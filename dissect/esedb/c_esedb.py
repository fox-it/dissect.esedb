import datetime
import uuid
import struct
from collections import namedtuple
from typing import Union

from dissect import cstruct

# https://github.com/microsoft/Extensible-Storage-Engine
c_esedb_def = """
#define MAX_COMPUTERNAME_LENGTH 15

typedef int64 DBTIME;
typedef ULONG OBJID;
typedef ULONG PGNO;
typedef ULONG JET_ENGINEFORMATVERSION;
typedef int64 XECHECKSUM;

enum CODEPAGE {
    UNICODE                 = 1200,
    WESTERN                 = 1252,
    ASCII                   = 20127
};

enum COMPRESSION_SCHEME {
    COMPRESS_NONE           = 0x0,
    COMPRESS_7BITASCII      = 0x1,
    COMPRESS_7BITUNICODE    = 0x2,
    COMPRESS_XPRESS         = 0x3,
    COMPRESS_SCRUB          = 0x4,
    COMPRESS_XPRESS9        = 0x5,
    COMPRESS_XPRESS10       = 0x6,
};

enum JET_coltyp {
    Nil                     = 0,
    Bit                     = 1,
    UnsignedByte            = 2,
    Short                   = 3,
    Long                    = 4,
    Currency                = 5,
    IEEESingle              = 6,
    IEEEDouble              = 7,
    DateTime                = 8,
    Binary                  = 9,
    Text                    = 10,
    LongBinary              = 11,
    LongText                = 12,
    SLV                     = 13,
    UnsignedLong            = 14,
    LongLong                = 15,
    GUID                    = 16,
    UnsignedShort           = 17,
    Max                     = 18,
};

enum SYSOBJ {
    Nil                     = 0,
    Table                   = 1,
    Column                  = 2,
    Index                   = 3,
    LongValue               = 4,
    Callback                = 5,
};

struct LOGTIME {
    BYTE        bSeconds;                           //  0 - 60
    BYTE        bMinutes;                           //  0 - 60
    BYTE        bHours;                             //  0 - 24
    BYTE        bDay;                               //  1 - 31
    BYTE        bMonth;                             //  0 - 11
    BYTE        bYear;                              //  current year - 1900

    BYTE        fTimeIsUTC:1;
    BYTE        bMillisecondsLow:7;

    BYTE        fReserved:1;                        // fOSSnapshot
    BYTE        bMillisecondsHigh:3;
    BYTE        fUnused:4;
}

struct LGPOS {
    USHORT      ib;                                 // must be the last so that lgpos can
    USHORT      isec;                               // index of disksec starting logsec
    LONG        lGeneration;                        // generation of logsec
};

struct SIGNATURE {
    ULONG       ulRandom;                           //  a random number
    LOGTIME     logtimeCreate;                      //  time db created, in logtime format
    CHAR        szComputerName[MAX_COMPUTERNAME_LENGTH + 1];  // where db is created
};

enum BKINFOTYPE : DWORD {
    backupNormal            = 0x0,                  // normal should be 0 for backward compatibility
    backupOSSnapshot,
    backupSnapshot,
    backupSurrogate,                                // should not be persisted in header, but persisted and used by log.
};

struct BKINFO {
    LGPOS       lgposMark;                          //  id for this backup
    LOGTIME     logtimeMark;                        //  timestamp of when le_lgposMark was logged
    ULONG       genLow;                             //  backup set's min lgen
    ULONG       genHigh;                            //  backup set's max lgen
};

struct DBFILEHDR {
    ULONG       ulChecksum;                         //  checksum of the 4k page
    ULONG       ulMagic;                            //  Magic number
    ULONG       ulVersion;                          //  version of DAE the db created (see ulDAEVersion)
    LONG        attrib;                             //  attributes of the db
//  16 bytes

    DBTIME      dbtimeDirtied;                      //  DBTime of this database
//  24 bytes

    SIGNATURE   signDb;                             //  (28 bytes) signature of the db (incl. creation time)
//  52 bytes

    ULONG       dbstate;                            //  consistent/inconsistent state
//  56 bytes

    LGPOS       lgposConsistent;                    //  null if in inconsistent state
    LOGTIME     logtimeConsistent;                  //  null if in inconsistent state
//  72 bytes

    LOGTIME     logtimeAttach;                      //  Last attach time
    LGPOS       lgposAttach;
//  88 bytes

    LOGTIME     logtimeDetach;                      //  Last detach time
    LGPOS       lgposDetach;
//  104 bytes

    ULONG       dbid;                               //  current db attachment
//  108 bytes

    SIGNATURE   signLog;                            //  log signature
//  136 bytes

    BKINFO      bkinfoFullPrev;                     //  Last successful full backup
//  160 bytes

    BKINFO      bkinfoIncPrev;                      //  Last successful Incremental backup
//  184 bytes                                       //  Reset when bkinfoFullPrev is set

    BKINFO      bkinfoFullCur;                      //  current backup. Succeed if a
//  208 bytes                                       //  corresponding pat file generated

    union {
        ULONG   m_ulDbFlags;
        BYTE    m_rgbDbFlags[4];
    };

    OBJID       objidLast;                          //  Object id used so far.

    //  NT version information. This is needed to decide if an index need
    //  be recreated due to sort table changes.

    DWORD       dwMajorVersion;                     //  OS version info
    DWORD       dwMinorVersion;
//  224 bytes

    DWORD       dwBuildNumber;
    LONG        lSPNumber;                          //  use 31 bit only

    ULONG       ulDaeUpdateMajor;                   //  used to track incremental database format updates that
                                                    //  are backward-compatible (see ulDAEUpdateMajorMax)

    ULONG       cbPageSize;                         //  database page size (0 = 4k pages)
//  240 bytes

    ULONG       ulRepairCount;                      //  number of times ErrREPAIRAttachForRepair has been called on this database
    LOGTIME     logtimeRepair;                      //  the date of the last time that repair was run
//  252 bytes

    BYTE        rgbReservedSignSLV[ 28 ];           //  signSLV signature of associated SLV file (obsolete)
//  280 bytes

    DBTIME      dbtimeLastScrub;                    //  last dbtime the database was zeroed out
//  288 bytes

    LOGTIME     logtimeScrub;                       //  the date of the last time that the database was zeroed out
//  296 bytes

    LONG        lGenMinRequired;                    //  the minimum log generation required for replaying the logs. Typically the checkpoint generation
//  300 bytes

    LONG        lGenMaxRequired;                    //  the maximum log generation required for replaying the logs. This is known as the waypoint in BF.
//  304 bytes

    LONG        cpgUpgrade55Format;                 //
    LONG        cpgUpgradeFreePages;                //
    LONG        cpgUpgradeSpaceMapPages;            //
//  316 bytes

    BKINFO      bkinfoSnapshotCur;                  //  Current snapshot.
//  340 bytes

    ULONG       ulCreateVersion;                    //  version of DAE that created db (debugging only)
    ULONG       ulCreateUpdate;
//  348 bytes

    LOGTIME     logtimeGenMaxCreate;                //  creation time of the genMax log file
//  356 bytes

    BKINFOTYPE  bkinfoTypeFullPrev;                 //  Type of Last successful full backup
    BKINFOTYPE  bkinfoTypeIncPrev;                  //  Type of Last successful Incremental backup

//  364 bytes
    ULONG       ulRepairCountOld;                   //  number of times ErrREPAIRAttachForRepair has been called on this database before the last defrag

    ULONG       ulECCFixSuccess;                    //  number of times a one bit error was fixed and resulted in a good page
    LOGTIME     logtimeECCFixSuccess;               //  the date of the last time that a one bit error was fixed and resulted in a good page
    ULONG       ulECCFixSuccessOld;                 //  number of times a one bit error was fixed and resulted in a good page before last repair

    ULONG       ulECCFixFail;                       //  number of times a one bit error was fixed and resulted in a bad page
    LOGTIME     logtimeECCFixFail;                  //  the date of the last time that a one bit error was fixed and resulted in a bad page
    ULONG       ulECCFixFailOld;                    //  number of times a one bit error was fixed and resulted in a bad page before last repair

    ULONG       ulBadChecksum;                      //  number of times a non-correctable ECC/checksum error was found
    LOGTIME     logtimeBadChecksum;                 //  the date of the last time that a non-correctable ECC/checksum error was found
    ULONG       ulBadChecksumOld;                   //  number of times a non-correctable ECC/checksum error was found before last repair

//  416 bytes

    LONG        lGenMaxCommitted;                   //  the last log generation to take active log records for this database.  Not
                                                    //  ensuring replay through this log generation will lose the D in ACID.
//  420 bytes
    BKINFO      bkinfoCopyPrev;                     //  Last successful Copy backup
    BKINFO      bkinfoDiffPrev;                     //  Last successful Differential backup, reset when bkinfoFullPrev is set

//  468 bytes
    BKINFOTYPE  bkinfoTypeCopyPrev;                 //  Type of Last successful Incremental backup
    BKINFOTYPE  bkinfoTypeDiffPrev;                 //  Type of Last successful Differential backup

// 476 bytes
    ULONG       ulIncrementalReseedCount;           //  number of times incremental reseed has been initiated on this database
    LOGTIME     logtimeIncrementalReseed;           //  the date of the last time that incremental reseed was initiated on this database
    ULONG       ulIncrementalReseedCountOld;        //  number of times incremental reseed was initiated on this database before the last defrag

    ULONG       ulPagePatchCount;                   //  number of pages patched in the database as a part of incremental reseed
    LOGTIME     logtimePagePatch;                   //  the date of the last time that a page was patched as a part of incremental reseed
    ULONG       ulPagePatchCountOld;                //  number of pages patched in the database as a part of incremental reseed before the last defrag

// 508 bytes
    QWORD       qwSortVersion;                      // DEPRECATED: In old versions had "default" (?English?) LCID version, in new versions has 0xFFFFFFFFFFFF.

//  516 bytes                                       // checksum during recovery state
    LOGTIME     logtimeDbscanPrev;                  // last checksum finish time (UTC - 1900y)
    LOGTIME     logtimeDbscanStart;                 // start time (UTC - 1900y)
    PGNO        pgnoDbscanHighestContinuous;        // current pgno

//  536 bytes
    LONG        lGenRecovering;                     //  the current log generation that we are doing recovery::redo for.

//  540 bytes

    ULONG       ulExtendCount;
    LOGTIME     logtimeLastExtend;
    ULONG       ulShrinkCount;
    LOGTIME     logtimeLastShrink;

//  564 bytes

    LOGTIME     logtimeLastReAttach;
    LGPOS       lgposLastReAttach;
//  580 bytes

    ULONG       ulTrimCount;
//  584 bytes

    SIGNATURE   signDbHdrFlush;                     //  random signature generated at the time of the last DB header flush
    SIGNATURE   signFlushMapHdrFlush;               //  random signature generated at the time of the last FM header flush
//  640 bytes

    LONG        lGenMinConsistent;                  //  the minimum log generation required to bring the database to a clean state
                                                    //  it might be different from lGenMinRequired, which also encompasses flush map consistency
//  644 bytes

    ULONG       ulDaeUpdateMinor;                   //  used to track incremental database format updates that
                                                    //  are forwards-compatible (see ulDAEUpdateMinorMax)
    JET_ENGINEFORMATVERSION efvMaxBinAttachDiagnostic;  //  Max version of engine binary that has attached this database.
                                                        //      NOTE: This is NOT necessarily the format the DB creates/should maintain if JET_paramEnableFormatVersion is set.
                                                        //      NOTE: ALSO this is NOT propagated via redo, intentionally to avoid dependency on this.  Use le_ulDaeUpdateMinor instead.
//  652 bytes

    PGNO        pgnoDbscanHighest;                  // highest pgno - used to avoid re-scanning pages
//  656 bytes

    LGPOS       lgposLastResize;                    // lgpos of the last database resize committed to the database.
                                                    // This is unset on older builds and it requires JET_efvLgposLastResize to start being populated.
                                                    // A database attachment sets this to le_lgposAttach, even though there isn't a real resize, so it can be
                                                    // be viewed as "resize checkpoint".
                                                    // A database extension or shrinkage updates this to the LGPOS of the operation.
                                                    // A clean databse detachment sets this to le_lgposConsistent.
                                                    // Incremental reseed may set this back if the first divergent log is lower than the current le_lgposLastResize.
//  664 bytes

    BYTE        rgbReserved[3];                     // keeping the le_filetype in the same
                                                    // place as log file header and checkpoint
                                                    // file header
                                                    // Remember: when updating logfile, checkpoint or
                                                    // flush map file headers, please check here
//  667 bytes

    //  WARNING: MUST be placed at this offset for
    //  uniformity with db/log headers
    ULONG       filetype;                           //  JET_filetypeDatabase or JET_filetypeStreamingFile
//  671 bytes

    BYTE        rgbReserved2[1];                    // For alignment
//  672 bytes

    LOGTIME     logtimeGenMaxRequired;
//  680 bytes

    LONG        lGenPreRedoMinConsistent;           //  the minimum log generation required to bring the database to a clean state (value overriden during the start of redo, 0 if not overriden).
    LONG        lGenPreRedoMinRequired;             //  the minimum log generation required for replaying the logs (value overriden during the start of redo, 0 if not overriden).
//  688 bytes

    SIGNATURE   signRBSHdrFlush;                    //  random signature generated at the time of the last RBS header flush
//  716 bytes

    ULONG       ulRevertCount;                      //  number of times revert has been initiated on this database using the revert snapshots.
    LOGTIME     logtimeRevertFrom;                  //  the date of the last time that revert was initiated on this database using the revert snapshots.
    LOGTIME     logtimeRevertTo;                    //  the date of the last time we were reverting this database to.
    ULONG       ulRevertPageCount;                  //  number of pages reverted on the database by the recent revert operation.
    LGPOS       lgposCommitBeforeRevert;            //  lgpos of the last commit on the database before a revert was done and requires JET_efvApplyRevertSnapshot to start being populated.
                                                    //  This will be set only after a revert is completed and will be used to ignore JET_errDbTimeTooOld on passive copies.
                                                    //  This is because as part of revert any new page reverts will cause the page to be zeroed out and thereby might be behind the active's dbtime for the page.
};

flag PAGE_FLAG : uint32 {
    // where we are in the BTree
    Root                    = 0x00000001,           // fPageRoot
    Leaf                    = 0x00000002,           // fPageLeaf
    ParentOfLeaf            = 0x00000004,           // fPageParentOfLeaf
    // special flags
    Empty                   = 0x00000008,           // fPageEmpty
    Repair                  = 0x00000010,           // fPageRepair
    // what type of tree we are in
    Primary                 = 0x00000000,           // fPagePrimary
    SpaceTree               = 0x00000020,           // fPageSpaceTree
    Index                   = 0x00000040,           // fPageIndex
    LongValue               = 0x00000080,           // fPageLongValue
    // type of BTree key validation
    NonUniqueKeys           = 0x00000400,           // fPageNonUniqueKeys
    // upgrade info
    NewRecordFormat         = 0x00000800,           // fPageNewRecordFormat
    NewChecksumFormat       = 0x00002000,           // fPageNewChecksumFormat
    Scrubbed                = 0x00004000,           // fPageScrubbed
};

struct PGHDR {
    XECHECKSUM  checksum;
    DBTIME      dbtimeDirtied;
    PGNO        pgnoPrev;
    PGNO        pgnoNext;
    OBJID       objidFDP;
    USHORT      cbFree;
    USHORT      cbUncommittedFree;
    USHORT      ibMicFree;
    USHORT      itagMicFree;
    PAGE_FLAG   fFlags;
};

struct PGHDR2 {
    // PGHDR       pghdr;
    XECHECKSUM  rgChecksum[3];
    PGNO        pgno;
    CHAR        rgbReserved[12];
};

flag TAG_FLAG : uint16 {
    Version                 = 0x01,                 // fNDVersion
    Deleted                 = 0x02,                 // fNDDeleted
    Compressed              = 0x04,                 // fNDCompressed
};

struct TAG {
    USHORT      cb_;                                // size
    USHORT      ib_;                                // offset
};

typedef WORD RECOFFSET;

struct RECHDR {
    BYTE        fidFixedLastInRec;
    BYTE        fidVarLastInRec;
    RECOFFSET   ibEndOfFixedData;
};

flag TAGFLD_HEADER : uint8 {
    Invalid                 = 0x00,                 // Not a real flag
    LongValue               = 0x01,                 // fLongValue, is the column type JET_coltypLongText or JET_coltypLongBinary
    Compressed              = 0x02,                 // fCompressed
    Separated               = 0x04,                 // fSeparated, is the data stored in a long value page
    MultiValues             = 0x08,                 // fMultiValues
    TwoValues               = 0x10,                 // fTwoValues
    Null                    = 0x20,                 // fNull
    Encrypted               = 0x40,                 // fEncrypted
};

flag JET_bitIndex : uint32 {
    Unique                  = 0x00000001,
    Primary                 = 0x00000002,
    DisallowNull            = 0x00000004,
    IgnoreNull              = 0x00000008,
    Clustered40             = 0x00000010,           // for backward compatibility
    IgnoreAnyNull           = 0x00000020,
    IgnoreFirstNull         = 0x00000040,
    LazyFlush               = 0x00000080,
    Empty                   = 0x00000100,           // don't attempt to build index, because all entries would evaluate to NULL (MUST also specify JET_bitIgnoreAnyNull)
    Unversioned             = 0x00000200,
    SortNullsHigh           = 0x00000400,           // NULL sorts after data for all columns in the index
    Unicode                 = 0x00000800,
    Tuples                  = 0x00001000,           // index on substring tuples (text columns only)
    TupleLimits             = 0x00002000,           // cbVarSegMac field of JET_INDEXCREATE actually points to a JET_TUPLELIMITS struct to allow custom tuple index limits (implies JET_bitIndexTuples)
    CrossProduct            = 0x00004000,           // index over multiple multi-valued columns has full cross product
    KeyMost                 = 0x00008000,           // custom index key size set instead of default of 255 bytes
    DisallowTruncation      = 0x00010000,           // fail update rather than truncate index keys
    NestedTable             = 0x00020000,           // index over multiple multi-valued columns but only with values of same itagSequence
    DotNetGuid              = 0x00040000,           // index over GUID column according to .Net GUID sort order
    ImmutableStructure      = 0x00080000,           // Do not write to the input structures during a JetCreateIndexN call.
};
"""  # noqa E501

c_esedb = cstruct.cstruct()
c_esedb.load(c_esedb_def)

ulDAEMagic = 0x89ABCDEF
pgnoFDPMSO = 4
pgnoFDPMSO_NameIndex = 7
pgnoFDPMSO_RootObjectIndex = 10
pgnoFDPMSOShadow = 24

JET_coltyp = c_esedb.JET_coltyp
JET_bitIndex = c_esedb.JET_bitIndex
SYSOBJ = c_esedb.SYSOBJ
PAGE_FLAG = c_esedb.PAGE_FLAG
TAG_FLAG = c_esedb.TAG_FLAG
TAGFLD_HEADER = c_esedb.TAGFLD_HEADER
CODEPAGE = c_esedb.CODEPAGE
COMPRESSION_SCHEME = c_esedb.COMPRESSION_SCHEME

CODEPAGE_MAP = {
    CODEPAGE.UNICODE: "utf-16-le",
    CODEPAGE.WESTERN: "cp1252",
    CODEPAGE.ASCII: "ascii",
}

RecordValue = Union[int, float, str, bytes, datetime.datetime, None]


def decode_bit(buf: bytes) -> bool:
    """Decode a bit into a boolean.

    Args:
        buf: The buffer to decode from.
    """
    return c_esedb.uint8(buf) == 0xFF


def decode_text(buf: bytes, encoding: CODEPAGE) -> str:
    """Decode text with the appropriate encoding.

    Args:
        buf: The buffer to decode from.
    """
    buf = bytes(buf)

    if encoding == CODEPAGE.UNICODE and len(buf) % 2:
        buf += b"\x00"

    return buf.decode(CODEPAGE_MAP[encoding]).rstrip("\x00")


def decode_guid(buf: bytes) -> str:
    """Decode a GUID.

    Args:
        buf: The buffer to decode from.
    """
    return str(uuid.UUID(bytes_le=bytes(buf)))


def checksum_xor(data: bytes, initial: int = 0x89ABCDEF) -> int:
    digest = initial
    for val in struct.unpack(f"<{len(data) // 4}I", data):
        digest ^= val

    return digest


ColumnType = namedtuple("ColumnType", ["value", "name", "size", "parse"])

COLUMN_TYPES = [
    ColumnType(JET_coltyp.Nil, "NULL", 0, None),
    ColumnType(JET_coltyp.Bit, "Boolean", 1, decode_bit),
    ColumnType(JET_coltyp.UnsignedByte, "Unsigned byte", 1, c_esedb.uint8),
    ColumnType(JET_coltyp.Short, "Signed short", 2, c_esedb.int16),
    ColumnType(JET_coltyp.Long, "Signed long", 4, c_esedb.int32),
    ColumnType(JET_coltyp.Currency, "Currency", 8, c_esedb.int64),
    ColumnType(JET_coltyp.IEEESingle, "Single precision FP", 4, c_esedb.float),
    ColumnType(JET_coltyp.IEEEDouble, "Double precision FP", 8, c_esedb.double),
    # Parse DateTime as an int64 because the actual parsing of the value can differ between databases
    # E.g. by default it's supposed to be an OA date, but the UAL stores it as a regular Windows timestamp
    ColumnType(JET_coltyp.DateTime, "DateTime", 8, c_esedb.int64),
    ColumnType(JET_coltyp.Binary, "Binary", None, bytes),
    ColumnType(JET_coltyp.Text, "Text", None, decode_text),
    ColumnType(JET_coltyp.LongBinary, "Long Binary", None, bytes),
    ColumnType(JET_coltyp.LongText, "Long Text", None, decode_text),
    ColumnType(JET_coltyp.SLV, "Super Long Value", None, None),
    ColumnType(JET_coltyp.UnsignedLong, "Unsigned long", 4, c_esedb.uint32),
    ColumnType(JET_coltyp.LongLong, "Signed Long long", 8, c_esedb.int64),
    ColumnType(JET_coltyp.GUID, "GUID", 16, decode_guid),
    ColumnType(JET_coltyp.UnsignedShort, "Unsigned short", 2, c_esedb.uint16),
    ColumnType(JET_coltyp.Max, "Max", None, None),
]
COLUMN_TYPE_MAP = {t.value.value: t for t in COLUMN_TYPES}
