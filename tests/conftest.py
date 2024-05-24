import gzip
import os
from typing import IO, BinaryIO, Iterator

import pytest


def absolute_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def open_file(name: str, mode: str = "rb") -> Iterator[IO]:
    with open(absolute_path(name), mode) as f:
        yield f


def open_file_gz(name: str, mode: str = "rb") -> Iterator[IO]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


@pytest.fixture
def basic_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/basic.edb.gz")


@pytest.fixture
def binary_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/binary.edb.gz")


@pytest.fixture
def text_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/text.edb.gz")


@pytest.fixture
def multi_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/multi.edb.gz")


@pytest.fixture
def default_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/default.edb.gz")


@pytest.fixture
def index_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/index.edb.gz")


@pytest.fixture
def large_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/large.edb.gz")


@pytest.fixture
def sru_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/SRUDB.dat.gz")


@pytest.fixture
def ual_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/Current.mdb.gz")
