"""Enumerates sorts of data that can exist"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, ClassVar

import fsspec

from intake.readers.utils import subclasses

# https://en.wikipedia.org/wiki/List_of_file_signatures


@dataclass
class BaseData:
    """Prototype dataset definition"""

    metadata: dict = field(default_factory=dict)
    mimetypes: ClassVar = set()
    filepattern: ClassVar = set()
    structure: ClassVar = None


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

    structure = "catalog"


class Parquet(FileData):
    filepattern = {"parq$", "parquet$", "/$"}
    mimetypes = {"application/vnd.apache.parquet"}
    structure = {"table", "nested"}
    magic = {b"PAR1"}


class CSV(FileData):
    filepattern = {"csv$", "txt$", "tsv$"}
    mimetypes = {"text/csv", "application/csv", "application/vnd.ms-excel"}
    structure = {"table"}


class Text(FileData):
    filepattern = {"txt$", "text$"}
    mimetypes = {"text/.*"}
    structure = {"sequence"}


@dataclass
class SQLQuery(Service):
    structure: ClassVar = {"sequence", "table"}
    conn: str | dict = ""
    query: str = ""
    filepattern = {"^oracle", "^mssql", "^sqlite", "^mysql", "^postgres"}


class CatalogFile(Catalog, FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}


@dataclass
class CatalogAPI(Catalog, Service):
    api_root: str = ""
    headers: dict = field(default_factory=dict)


class YAMLFile(FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    filepattern = {"json$"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}


@dataclass()
class Tiled(Service):
    tiled_client: Any = None


comp_magic = {
    # These are a bit like datatypes making raw bytes/file object output
    (0, b"\x1f\x8b"): "gzip",
    (0, b"BZh"): "bzip2",
    (0, b"(\xc2\xb5/\xc3\xbd"): "zstd",
}
container_magic = {
    # these are like datatypes making filesystems
    (257, b"ustar"): "tar",
    (0, b"PK"): "zip",
}


def recommend(url=None, mime=None, head=None, storage_options=None):
    """Show which data types match

    Parameters
    ----------
    url: str
        Location of data
    mime: str
        MIME type, usually "x/y" form
    head: bytes
        A small number of bytes from the file head, for seeking magic bytes
    storage_options: dict | None
        If passing a URL which might be a remote file, storage_options can be used
        by fsspec.

    Returns
    -------
    set of matching datatype classes
    """
    # instead of returning datatype class, should we make instance with
    # storage_options, if known?
    out = set()
    if mime:
        for cls in subclasses(BaseData):
            if any(re.match(m, mime) for m in cls.mimetypes):
                out.add(cls)
    if url:
        # urlparse to remove query parts?
        # try stripping compression extensions?
        for cls in subclasses(BaseData):
            if any(re.findall(m, url) for m in cls.filepattern):
                out.add(cls)
    if head:
        for cls in subclasses(FileData):
            if any(head.startswith(m) for m in cls.magic):
                out.add(cls)
    if out or url:
        return out

    try:
        with fsspec.open(url, "rb", **(storage_options or {})) as f:
            head = f.read(2**20)
    except IOError:
        return out

    for (off, mag), comp in comp_magic:
        if head[off:].startswith(mag):
            storage_options = (storage_options or {}).copy()
            storage_options["compression"] = comp
            out = recommend(url, storage_options=storage_options)
            if out:
                print("Update storage_options: ", storage_options)
                return out
    for (off, mag), comp in container_magic:
        if head[off:].startswith(mag):
            prot = fsspec.core.split_protocol(url)[0]
            out = recommend(f"{comp}://*::{url}", storage_options={prot: storage_options})
            if out:
                print("Update url: ", url, "\nstorage_options: ", storage_options)
                return out
    return recommend(head=head)
