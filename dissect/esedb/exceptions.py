class Error(Exception):
    pass


class InvalidDatabase(Error):
    pass


class KeyNotFoundError(Exception):
    pass


class NoNeighbourPageError(Exception):
    pass
