"""Enumerates sorts of data that can exist"""

from __future__ import annotations

import re
from typing import Any

import fsspec

from intake import import_name
from intake.readers.utils import Tokenizable, subclasses

# TODO: make "structure" possibilities an enum?

# https://en.wikipedia.org/wiki/List_of_file_signatures


class BaseData(Tokenizable):
    """Prototype dataset definition"""

    mimetypes: set[str] = set()
    filepattern: set[str] = set()
    structure: set[str] = set()

    def __init__(self, metadata: dict[str, Any] | None = None):
        self.metadata: dict[str, Any] = metadata or {}  # arbitrary information

    @property
    def possible_readers(self):
        """List of reader classes for this type, grouped by importability"""
        from intake.readers.readers import recommend

        return recommend(self)

    @property
    def possible_outputs(self):
        readers = self.possible_readers["importable"]
        return {r: r.output_instance for r in readers}

    def to_reader(self, outtype: str | None = None, reader: str | type | None = None):
        if outtype and reader:
            raise ValueError
        if isinstance(reader, str):
            reader = import_name(reader)
        if reader:
            return reader(data=self)
        elif outtype:
            for reader, out in self.possible_outputs.items():
                if out == outtype or re.match(outtype, out):
                    return reader(data=self)
            raise ValueError("outtype not in available in importable readers")
        reader = next(iter(self.possible_readers["importable"]))
        return reader(data=self)

    def to_entry(self):
        from intake.readers.entry import DataDescription

        kw = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return DataDescription(datatype=self.qname(), kwargs=kw, metadata=self.metadata)

    def __repr__(self):
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{type(self).__name__}, {d}"


class FileData(BaseData):
    """Datatypes loaded from files"""

    magic = set()  # bytes at file start to identify it

    def __init__(self, url: str = "", storage_options: dict | None = None, metadata: dict | None = None):
        self.url = url
        self.storage_options = storage_options
        self._filelist = None
        super().__init__(metadata)

    @property
    def filelist(self):
        """Expand any globs in the given URL"""
        if self._filelist is None:
            if isinstance(self.url, (list, tuple)):
                self._filelist = self.url
            else:
                self._filelist = fsspec.core.get_fs_token_paths(self.url, storage_options=self.storage_options)[2]
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


class PNG(FileData):
    filepattern = {"png$"}
    structure = {"array", "image"}
    mimetypes = {"image/png"}
    magic = {b"\x89PNG"}


class SQLQuery(Service):
    structure = {"sequence", "table"}
    filepattern = {"^oracle", "^mssql", "^sqlite", "^mysql", "^postgres"}

    def __init__(self, conn, query, metadata=None):
        self.conn = conn
        self.query = query
        super().__init__(metadata)


class CatalogFile(Catalog, FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}


class CatalogAPI(Catalog, Service):
    def __init__(self, api_root, headers=None, metadata=None):
        self.api_root = api_root
        self.headers = headers
        super().__init__(metadata=metadata)


class YAMLFile(FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    filepattern = {"json$"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}


class Tiled(Service):
    def __init__(self, tiled_client, metadata=None):
        self.tiled_client = tiled_client
        super().__init__(metadata)


class ReaderData(BaseData):
    """Represents the output of another reader as a data entity

    This type is special, as it will lead to a reference being created when any
    reader using it is included in a catalog.
    """

    def __init__(self, reader, metadata=None):
        self.reader = reader
        super().__init__(metadata)


class NumpyFile(FileData):
    magic = {b"\x93NUMPY"}
    filepattern = {"npy$", "text$"}
    structure = {"array"}


class Feather2(FileData):
    magic = {b"ARROW1"}
    structure = {"tabular", "structured"}


class Feather1(FileData):
    magic = {b"FEA1"}
    structure = {"tabular", "structured"}


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
