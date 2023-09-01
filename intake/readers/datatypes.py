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

    def to_reader_cls(self, outtype: str | None = None, reader: str | type | None = None):
        if outtype and reader:
            raise ValueError
        if isinstance(reader, str):
            try:
                reader = import_name(reader)
            except (ImportError, ModuleNotFoundError):
                for cls, out in self.possible_outputs.items():
                    if re.findall(reader, cls.qname()):
                        return cls
        if reader:
            return reader
        elif outtype:
            for reader, out in self.possible_outputs.items():
                if out == outtype or re.findall(outtype, out):
                    return reader
            raise ValueError("outtype not in available in importable readers")
        return next(iter(self.possible_readers["importable"]))

    def to_reader(self, outtype: str | None = None, reader: str | type | None = None):
        return self.to_reader_cls(outtype, reader)(data=self)

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

    def __init__(self, url, storage_options: dict | None = None, metadata: dict | None = None):
        self.url = url
        self.storage_options = storage_options
        self._filelist = None
        super().__init__(metadata)


class Service(BaseData):
    """Datatypes loaded from some service"""

    def __init__(self, url, options=None, metadata=None):
        self.url = url
        self.options = options or {}
        super().__init__(metadata=metadata)


class Catalog(BaseData):
    """Datatypes that are groupings of other data"""

    structure = {"catalog"}


class Parquet(FileData):
    filepattern = {"parq$", "parquet$", "/$"}
    mimetypes = {"application/vnd.apache.parquet", "application/parquet", "application/x-parquet"}
    structure = {"table", "nested"}
    magic = {b"PAR1"}


class CSV(FileData):
    filepattern = {"csv$", "txt$", "tsv$"}
    mimetypes = {"text/csv", "application/csv", "application/vnd.ms-excel"}
    structure = {"table"}


class Text(FileData):
    filepattern = {"txt$", "text$", "dat$", "ascii$"}
    mimetypes = {"text/.*"}
    structure = {"sequence"}


class XML(FileData):
    filepattern = {"xml[sx]?$"}
    mimetypes = {"application/xml", "text/xml"}
    structure = {"nested"}
    magic = {b"<?xml "}


class PNG(FileData):
    filepattern = {"png$"}
    structure = {"array", "image"}
    mimetypes = {"image/png"}
    magic = {b"\x89PNG"}


class NetCDF3(FileData):
    filepattern = {"nc$", "netcdf3$", "nc3$"}
    structure = {"array"}
    mimetypes = {"application/x-netcdf"}
    magic = {b"CDF"}


class HDF5(FileData):
    filepattern = {"hdf$", "h4$", "hdf5$"}  # many others by convention
    structure = {"array", "table", "hierarchy"}
    magic = {b"HDF"}
    mimetypes = {"application/x-hdf5?"}


class Zarr(FileData):
    filepattern = {"zarr$", "/$"}  # i.e., any directory might be
    structure = {"array", "hierarchy"}
    mimetypes = {"application/vnd\\+zarr"}


class TIFF(FileData):
    # includes geoTIFF/COG, or split out?
    filepattern = {"tiff?$", "cog$"}
    structure = {"array", "image"}
    magic = {b"II*\x00", b"MM\x00*"}
    mimetypes = {"image/tiff", "image/x.geotiff"}


class GRIB2(FileData):
    filepattern = {"grib2?$"}
    structure = {"array"}
    magic = {b"GRIB"}
    mimetypes = {"application/wmo-grib"}


class FITS(FileData):
    filepattern = {"fits$"}  # other conventions too
    structure = {"array", "table"}
    magic = {b"SIMPLE"}
    mimetypes = {"image/fits", "application/fits"}


class ASDF(FileData):
    filepattern = {"asdf$"}
    structure = {"array", "table"}
    magic = {b"#ASDF"}


class DICOM(FileData):
    filepattern = {"dicom$", "dcm$"}  # and others
    structure = {"array", "image"}
    magic = {(128, b"DICM")}
    mimetypes = {"application/dicom"}


class Nifti(FileData):
    filepattern = {"nii$", "nii.gz$"}
    structure = {"array", "image"}
    magic = {(344, b"\x6E\x69\x31\x00"), (344, b"\x6E\x2B\x31\x00")}


class OpenDAP(Service):
    structure = {"array"}


class SQLQuery(Service):
    structure = {"sequence", "table"}
    filepattern = {"^oracle", "^mssql", "^sqlite", "^mysql", "^postgres"}

    def __init__(self, conn, query, metadata=None):
        self.conn = conn
        self.query = query
        super().__init__(metadata)


class SQLite(FileData):
    structure = {"sequence", "table"}
    filepattern = {"sqlite$", "sqlitedb$", "db$"}
    magic = {b"SQLite format"}


class CatalogFile(Catalog, FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}


class CatalogAPI(Catalog, Service):
    filepattern = {"^(http|https):"}
    structure = {"catalog"}


class YAMLFile(FileData):
    filepattern = {"yaml$", "yml$"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    filepattern = {"json$"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}


class STACJSON(JSONFile):
    magic = {(None, b'"stac_version":')}  # None means "somewhere in the file head"


class TiledService(CatalogAPI):
    ...


class TiledDataset(Service):
    structure = {"array", "table", "nested"}


class ReaderData(BaseData):
    """Represents the output of another reader as a data entity

    This type is special, as it will lead to a reference being created when any
    reader using it is included in a catalog.
    """

    def __init__(self, reader, metadata=None):
        self.reader = reader
        super().__init__(metadata)


class NumpyFile(FileData):
    # will also match .npz since it will be recognised as a ZIP archive
    magic = {b"\x93NUMPY"}
    filepattern = {"npy$", "text$"}
    structure = {"array"}


class RawBuffer(FileData):
    """A C or FORTRAN N-dimensional array buffer without metadata"""

    filepattern = {"raw$"}
    structure = {"array"}

    def __init__(self, url: str, dtype: str, storage_options: dict | None = None, metadata: dict | None = None):
        super().__init__(url, storage_options=storage_options, metadata=metadata)
        self.dtype = dtype  # numpy-style


class Literal(BaseData):
    """A value that can be embedded directly to YAML (text, dict, list)"""

    def __init__(self, data, metadata=None):
        self.data = data
        super().__init__(metadata=metadata)


class Feather2(FileData):
    magic = {b"ARROW1"}
    structure = {"tabular", "structured"}


class Feather1(FileData):
    magic = {b"FEA1"}
    structure = {"tabular", "structured"}


class PythonSourceCode(FileData):
    structure = {"code"}
    filepattern = {"py$"}


class GDALRasterFile(FileData):
    """One of the filetpes at https://gdal.org/drivers/raster/index.html

    This class overlaps with some other types, so only use when necessary.
    These must be local paths or use GDAL's own virtual file system.
    """

    structure = {"array"}


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
    out = []
    if head:
        for cls in subclasses(FileData):
            for m in cls.magic:
                if isinstance(m, tuple):
                    off, m = m
                    if off is None:
                        if m in head:
                            out.append(cls)
                            break
                else:
                    off = 0
                if off is not None and head[off:].startswith(m):
                    out.append(cls)
                    break
    if mime:
        for cls in subclasses(BaseData):
            if any(re.match(m, mime) for m in cls.mimetypes):
                out.append(cls)
    if url:
        # urlparse to remove query parts?
        # try stripping compression extensions?
        for cls in subclasses(BaseData):
            if any(re.findall(m, url.lower()) for m in cls.filepattern):
                out.append(cls)
    if out:
        return out

    if head is None and mime is None and url:
        try:
            fs, url2 = fsspec.core.url_to_fs(url, **(storage_options or {}))
            mime = fs.info(url2, refresh=True).get("ContentType", None)
        except (IOError, TypeError, AttributeError):
            mime = None
        try:
            with fsspec.open(url, "rb", **(storage_options or {})) as f:
                head = f.read(2**20)
        except IOError:
            head = None
        if mime or head:
            return recommend(url, mime=mime, head=head, storage_options=storage_options)
        return out

    for (off, mag), comp in comp_magic.items():
        if head[off:].startswith(mag):
            storage_options = (storage_options or {}).copy()
            storage_options["compression"] = comp
            out = recommend(url, storage_options=storage_options)
            if out:
                print("Update storage_options: ", storage_options)
                return out
    for (off, mag), comp in container_magic.items():
        if head[off:].startswith(mag):
            prot = fsspec.core.split_protocol(url)[0]
            out = recommend(f"{comp}://*::{url}", storage_options={prot: storage_options})
            if out:
                print("Update url: ", url, "\nstorage_options: ", storage_options)
                return out
