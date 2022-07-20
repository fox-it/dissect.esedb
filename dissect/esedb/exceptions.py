""" Custom module exceptions """


class Error(Exception):
    """Base class for exceptions for this module.
    It used to recognize errors specific to this module"""

    pass


class InvalidDatabase(Error):
    pass


class InvalidPageNumber(Error):
    pass


class InvalidTagNumber(Error):
    pass


class InvalidTable(Error):
    pass


class InvalidColumn(Error):
    pass


class CompressedTaggedDataError(Error):
    """Occurs when the tagged data is compressed in a record."""

    pass
