class Error(Exception):
    pass


class InvalidDatabase(Error):
    pass


class KeyNotFoundError(Error):
    pass


class NoNeighbourPageError(Error):
    pass
