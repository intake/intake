"""Enumerates sorts of data that can exist"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import ClassVar

import fsspec

from intake.readers.utils import subclasses


@dataclass
class BaseData:
    """Prototype dataset definition"""

    metadata: dict = field(default_factory=dict)
    mimetypes: ClassVar = set()
    extensions: ClassVar = set()


@dataclass
class FileData(BaseData):
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


class Service(BaseData):
    """Datatypes loaded from some service"""

    ...


class Catalog(BaseData):
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
    conn: str | dict = ""
    query: str = ""


class CatalogFile(Catalog, FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}


@dataclass
class CatalogAPI(Catalog, Service):
    api_root: str = ""
    headers: dict = field(default_factory=dict)


class YAMLFile(FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    extensions = {"json"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}


def recommend(url=None, mime=None, head=None, import_all=False):
    """Show which data types match

    Parameters
    ----------
    url: str
        Location of data
    mime: str
        MIME type, usually "x/y" form
    head: bytes
        A small number of bytes from the file head, for seeking magic bytes
    import_all: bool
        If true, will load all entrypoint definitions of data types, which will not
    show up if they have not yet been imported

    Returns
    -------
    set of matching datatype classes
    """
    out = set()
    if import_all:
        pass  # scan entrypoints for data types
    if mime:
        for cls in subclasses(BaseData):
            if any(re.match(m, mime) for m in cls.mimetypes):
                out.add(cls)
    if url:
        # urlparse to remove query parts?
        for cls in subclasses(BaseData):
            if any(url.endswith(m) for m in cls.extensions):
                out.add(cls)
    if head:
        for cls in subclasses(FileData):
            if any(head.startswith for m in cls.magic):
                out.add(cls)

    return out
