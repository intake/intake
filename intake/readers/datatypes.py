from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import ClassVar

import fsspec

from intake.readers.utils import subclasses


@dataclass
class Base:
    """Prototype dataset definition"""

    kwargs: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    mimetypes: ClassVar = set()
    extensions: ClassVar = set()


@dataclass
class FileData(Base):
    """Datatypes loaded from files"""

    url: str | list = ""  # location of the dataset
    storage_option: dict = field(default_factory=dict)  # any fsspec kwargs to read that location
    _filelist: ClassVar[list | None] = None  # will hold list of files after glob expansion
    magic = set()  # bytes at file start to identify it

    @property
    def filelist(self):
        """Expand any globs in the given URL"""
        if self._filelist is None:
            if isinstance(self.url, (list, tuple)):
                self._filelist = self.url
            else:
                self._filelist = fsspec.core.get_fs_token_paths(self.url, storage_options=self.storage_option)[2]
        return self._filelist


class Service:
    """Datatypes loaded from some service"""

    ...


class Catalog:
    """Datatypes that are groupings of other data"""

    ...


class Parquet(FileData):
    extensions = {"parq", "parquet", "/"}
    mimetypes = {"application/vnd.apache.parquet"}
    structure = {"table", "nested"}
    magic = {b"PAR1"}


class CSV(FileData):
    extensions = {"csv", "txt", "tsv"}
    mimetypes = {"text/csv", "application/csv", "application/vnd.ms-excel"}
    structure = {"table"}


class Text(FileData):
    extensions = {"txt", "text"}
    mimetypes = {"text/.*"}
    structure = {"sequence"}


@dataclass
class SQLQuery(Service):
    structure: ClassVar = {"sequence", "table"}
    conn: str | dict
    query: str


class CatalogFile(Catalog, FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}


@dataclass
class CatalogAPI(Catalog, Service):
    api_root: str


class YAMLFile(FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    extensions = {"json"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}


def recommend(url=None, mime=None, head=None):
    """Show which data types match

    Parameters
    ----------
    url: str
        Location of data
    mime: str
        MIME type, usually "x/y" form
    head: bytes
        A small number of bytes from the file head, for seeking magic bytes

    Returns
    -------
    set of matching datatype classes
    """
    out = set()
    if mime:
        for cls in subclasses(Base):
            if any(re.match(m, mime) for m in cls.mimetypes):
                out.add(cls)
    if url:
        # urlparse to remove query parts?
        for cls in subclasses(Base):
            if any(url.endswith(m) for m in cls.extensions):
                out.add(cls)
    if head:
        for cls in subclasses(FileData):
            if any(head.startswith for m in cls.magic):
                out.add(cls)

    return out
