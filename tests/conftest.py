from __future__ import annotations

import gzip
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def absolute_path(filename: str) -> Path:
    return Path(__file__).parent / filename


def open_file(name: str, mode: str = "rb") -> Iterator[BinaryIO]:
    with absolute_path(name).open(mode) as f:
        yield f


def open_file_gz(name: str, mode: str = "rb") -> Iterator[BinaryIO]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


@pytest.fixture
def basic_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/basic.edb.gz")


@pytest.fixture
def binary_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/binary.edb.gz")


@pytest.fixture
def text_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/text.edb.gz")


@pytest.fixture
def multi_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/multi.edb.gz")


@pytest.fixture
def default_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/default.edb.gz")


@pytest.fixture
def index_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/index.edb.gz")


@pytest.fixture
def large_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/large.edb.gz")


@pytest.fixture
def sru_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/SRUDB.dat.gz")


@pytest.fixture
def ual_db() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/Current.mdb.gz")
