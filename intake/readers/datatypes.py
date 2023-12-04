"""Enumerates sorts of data that can exist"""

from __future__ import annotations

import re
from functools import cache
from typing import Any

import fsspec

from intake import import_name
from intake.readers.utils import Tokenizable, subclasses

# TODO: make "structure" possibilities an enum?


# https://en.wikipedia.org/wiki/List_of_file_signatures


class BaseData(Tokenizable):
    """Prototype dataset definition"""

    mimetypes: str = ""  # regex
    filepattern: str = ""  # regex
    structure: set[str] = set()

    def __init__(self, metadata: dict[str, Any] | None = None):
        self.metadata: dict[str, Any] = metadata or {}  # arbitrary information

    @classmethod
    @cache
    def _filepattern(cls):
        return re.compile(cls.filepattern)

    @classmethod
    @cache
    def _mimetypes(cls):
        return re.compile(cls.mimetypes)

    @property
    def possible_readers(self):
        """List of reader classes for this type, grouped by importability"""
        from intake.readers.readers import recommend

        return recommend(self)

    @property
    def possible_outputs(self):
        """Map of importable readers to the expected output class of each"""
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
        if isinstance(reader, type):
            return reader
        elif outtype:
            for reader, out in self.possible_outputs.items():
                if out is not None and (out == outtype or re.findall(outtype, out)):
                    return reader
            raise ValueError("outtype not in available in importable readers")
        return next(iter(self.possible_readers["importable"]))

    def to_reader(self, outtype: str | None = None, reader: str | None = None, **kw):
        """Find an appropriate reader for this data

        If neither ``outtype`` or ``reader`` is passed, the first importable reader
        will be picked.

        See also ``.possible_outputs``

        Parameters
        ----------
        outtype: string to match against the output classes of potential readers
        reader: string to match against the class names of the readers
        """
        return self.to_reader_cls(outtype, reader)(data=self, **kw)

    def to_entry(self):
        """Create DataDescription version of this, for placing in a Catalog"""
        from intake.readers.entry import DataDescription

        kw = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return DataDescription(datatype=self.qname(), kwargs=kw, metadata=self.metadata)

    def __repr__(self):
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{type(self).__name__}, {d}"

    def auto_pipeline(self, outtype):
        from intake.readers.convert import auto_pipeline

        return auto_pipeline(self, outtype)


class FileData(BaseData):
    """Datatypes loaded from files"""

    magic = set()  # bytes at file start to identify it

    def __init__(self, url, storage_options: dict | None = None, metadata: dict | None = None):
        self.url = url
        self.storage_options = storage_options
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
    filepattern = "(parq|parquet|/$)"
    mimetypes = "application/(vnd.apache.parquet|parquet|x-parquet)/"
    structure = {"table", "nested"}
    magic = {b"PAR1"}


class CSV(FileData):
    filepattern = "(csv$|txt$|tsv$)"
    mimetypes = "(text/csv|application/csv|application/vnd.ms-excel)"
    structure = {"table"}


class Text(FileData):
    filepattern = "(txt$|text$|dat$|ascii$)"
    mimetypes = "text/.*"
    structure = {"sequence"}
    # some optional byte order marks
    # https://en.wikipedia.org/wiki/Byte_order_mark#Byte_order_marks_by_encoding
    magic = {b"\xEF\xBB\xBF", b"\xFE\xFF", b"\xFF\xFE", b"\x00\x00\xFE\xFF"}


class XML(FileData):
    filepattern = "xml[sx]?$"
    mimetypes = "(application|text)/xml"
    structure = {"nested"}
    magic = {b"<?xml "}


class THREDDSCatalog(XML):
    magic = {(None, b"<xml.*<catalog ")}
    structure = {"catalog"}


class PNG(FileData):
    filepattern = "png$"
    structure = {"array", "image"}
    mimetypes = "image/png"
    magic = {b"\x89PNG"}


class JPEG(FileData):
    filepattern = "jpe?g$"
    structure = {"array", "image"}
    mimetypes = "image/jpeg"
    magic = {b"\xFF\xD8\xFF"}


class WAV(FileData):
    filepattern = "wav$"
    structure = {"array", "timeseries"}
    mimetypes = "audio/wav"
    magic = {(8, b"WAVE")}


class NetCDF3(FileData):
    filepattern = "(netcdf3$|nc3?$)"
    structure = {"array"}
    mimetypes = "application/x-netcdf"
    magic = {b"CDF"}


class HDF5(FileData):
    filepattern = "(hdf5?|h4|nc4?)$"  # many others by convention
    structure = {"array", "table", "hierarchy"}
    magic = {b"HDF"}
    mimetypes = "application/x-hdf5?"

    def __init__(
        self,
        url,
        storage_options: dict | None = None,
        path: str = "",
        metadata: dict | None = None,
    ):
        self.url = url
        self.storage_options = storage_options
        self.path = path
        super().__init__(url=url, storage_options=storage_options, metadata=metadata)


class Zarr(FileData):
    filepattern = "(zarr$|/$)"  # i.e., any directory might be
    structure = {"array", "hierarchy"}
    mimetypes = "application/vnd\\+zarr"

    def __init__(
        self,
        url,
        storage_options: dict | None = None,
        root: str = "",
        metadata: dict | None = None,
    ):
        self.url = url
        self.storage_options = storage_options
        self.root = root
        super().__init__(url=url, storage_options=storage_options, metadata=metadata)


class MatlabArray(FileData):
    """A single array in a .mat file"""

    filepattern = "mat$"
    magic = {b"MATLAB"}

    def __init__(self, path, variable=None):
        """If variable is None, takes first non-underscored variable found"""
        self.path = path
        self.variable = variable


class MatrixMarket(FileData):
    magic = {b"%%MatrixMarket"}


class Excel(FileData):
    filepattern = "xls[xmb]?"
    structure = {"tabular"}
    mimetypes = "application/.*(excel|xls)"
    magic = {b"\x50\x4B\x03\x04", b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"}  # will match any office doc


class TIFF(FileData):
    # includes geoTIFF/COG, or split out?
    filepattern = "(tiff?$|cog$)"
    structure = {"array", "image"}
    magic = {b"II*\x00", b"MM\x00*"}
    mimetypes = "image/(geo)?tiff"


class GRIB2(FileData):
    filepattern = "gri?b2?$"
    structure = {"array"}
    magic = {b"GRIB"}
    mimetypes = "application/wmo-grib"


class FITS(FileData):
    filepattern = "fits$"  # other conventions too
    structure = {"array", "table"}
    magic = {b"SIMPLE"}
    mimetypes = "(image|application)/fits"


class ASDF(FileData):
    filepattern = "asdf$"
    structure = {"array", "table"}
    magic = {b"#ASDF"}


class DICOM(FileData):
    filepattern = "(dicom|dcm|ct|mri|DCM)$"  # and others
    structure = {"array", "image"}
    magic = {(128, b"DICM")}
    mimetypes = "application/dicom"


class Nifti(FileData):
    # https://brainder.org/2012/09/23/the-nifti-file-format/
    filepattern = "(hdr|nii)(\\.gz)?$"
    structure = {"array", "image"}
    magic = {(344, b"\x6E\x69\x31\x00"), (344, b"\x6E\x2B\x31\x00")}


class OpenDAP(Service):
    structure = {"array"}


class SQLQuery(Service):
    structure = {"sequence", "table"}
    filepattern = "^(oracle|mssql|sqlite|mysql|postgres)"

    def __init__(self, conn, query, metadata=None):
        self.conn = conn
        self.query = query
        super().__init__(metadata)


class SQLite(FileData):
    structure = {"sequence", "table"}
    filepattern = "sqlite$|sqlitedb$|db$"
    magic = {b"SQLite format"}


class AVRO(FileData):
    structure = {"nested"}
    filepattern = "avro$"
    magic = {b"Obj\x01"}
    mimetypes = "avro/binary"


class ORC(FileData):
    structure = {"nested", "tabular"}
    filepattern = "orc$"
    magic = {b"ORC"}


class YAMLFile(FileData):
    filepattern = "ya?ml$"
    mimetypes = "text/yaml"
    structure = {"nested"}


class CatalogFile(Catalog, YAMLFile):
    ...


class CatalogAPI(Catalog, Service):
    filepattern = "^https?:"


class JSONFile(FileData):
    filepattern = "json$"
    mimetypes = "(text|application)/json"
    structure = {"nested", "table"}
    magic = {b"{"}


class GeoJSON(JSONFile):
    filepattern = "(?:geo)?json$"
    magic = {(None, b'"type": "Feature')}  # not guaranteed, but good indicator


class Shapefile(FileData):
    # this would only be found as a member of a .ZIP, since you need all three mandatory
    # files to make a dataset https://en.wikipedia.org/wiki/Shapefile#Overview
    # However, Fiona can read some .shp files with env SHAPE_RESTORE_SHX=YES
    filepattern = "shp$|shx$|dbf$"
    mimetypes = "x-gis/x-shapefile"
    magic = {b"\x00\x00\x27\x0a"}


class STACJSON(JSONFile):
    magic = {(None, b'"stac_version":')}  # None means "somewhere in the file head"
    mimetypes = "(text|application)/geo\\+json"


class TiledService(CatalogAPI):
    magic = {(None, b"<title>Tiled</title>")}


class TiledDataset(Service):
    structure = {"array", "table", "nested"}


class NumpyFile(FileData):
    # will also match .npz since it will be recognised as a ZIP archive
    magic = {b"\x93NUMPY"}
    filepattern = "(npy$|text$)"
    structure = {"array"}


class RawBuffer(FileData):
    """A C or FORTRAN N-dimensional array buffer without metadata"""

    filepattern = "raw$"
    structure = {"array"}

    def __init__(
        self,
        url: str,
        dtype: str,
        storage_options: dict | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(url, storage_options=storage_options, metadata=metadata)
        self.dtype = dtype  # numpy-style


class Literal(BaseData):
    """A value that can be embedded directly to YAML (text, dict, list)"""

    def __init__(self, data, metadata=None):
        self.data = data
        super().__init__(metadata=metadata)


class Feather2(FileData):
    magic = {b"ARROW1"}
    structure = {"tabular", "nested"}


class Feather1(FileData):
    magic = {b"FEA1"}
    structure = {"tabular", "nested"}


class PythonSourceCode(FileData):
    structure = {"code"}
    filepattern = "py$"


class GDALRasterFile(FileData):
    """One of the filetpes at https://gdal.org/drivers/raster/index.html

    This class overlaps with some other types, so only use when necessary.
    These must be local paths or use GDAL's own virtual file system.
    """

    structure = {"array"}


class GDALVectorFile(FileData):
    """One of the filetypes at https://gdal.org/drivers/vector/index.html

    This class overlaps with some other types, so only use when necessary.
    These must be local paths or use GDAL's own virtual file system.
    """

    structure = {
        "nested",
        "tabular",
    }  # tabular when read by geopandas, could be called a conversion


class HuggingfaceDataset(BaseData):
    """https://github.com/huggingface/datasets"""

    structure = {"nested", "text"}

    def __init__(self, name, split=None, metadata=None):
        super().__init__(metadata)
        self.name = name
        self.split = split


class TFRecord(FileData):
    structure = {"nested"}
    filepattern = "tfrec$"


class KerasModel(FileData):
    structure = {"model"}  # complex
    filepattern = "pb$"  # possibly protobuf


class PickleFile(FileData):
    structure = {}


class SKLearnPickleModel(PickleFile):
    ...


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


def recommend(url=None, mime=None, head=True, storage_options=None, ignore=None) -> set:
    """Show which data types match

    Parameters
    ----------
    url: str
        Location of data
    mime: str
        MIME type, usually "x/y" form
    head: bytes | bool | None
        A small number of bytes from the file head, for seeking magic bytes. If it is
        True, fetch these bytes from th given URL/storage_options and use them. If None,
        only fetch bytes if there is no match by mime type or path, if False, don't
        fetch at all.
    storage_options: dict | None
        If passing a URL which might be a remote file, storage_options can be used
        by fsspec.
    ignore: set | None
        Don't include these in the output

    Returns
    -------
    set of matching datatype classes.
    """
    # TODO: more complex returns defining which type of match hit what, or some kind of score
    outs = ignore or set()
    out = []
    if isinstance(url, (list, tuple)):
        url = url[0]
    if head is True and url:
        try:
            fs, url2 = fsspec.core.url_to_fs(url, **(storage_options or {}))
            mime = mime or fs.info(url2, refresh=True).get("ContentType", None)
        except (IOError, TypeError, AttributeError, ValueError):
            mime = mime or None
        try:
            with fsspec.open_files(url, "rb", **(storage_options or {}))[0] as f:
                head = f.read(2**20)
        except (IOError, IndexError):
            head = False

    if isinstance(head, bytes):
        for cls in subclasses(FileData):
            if cls in outs:
                continue
            for m in cls.magic:
                if isinstance(m, tuple):
                    off, m = m
                    if off is None:
                        if re.findall(m, head):
                            out.append(cls)
                            outs.add(cls)
                            break
                else:
                    off = 0
                if off is not None and head[off:].startswith(m):
                    out.append(cls)
                    outs.add(cls)
                    break
    if mime:
        for cls in subclasses(BaseData):
            if cls not in outs and cls.mimetypes and re.match(cls._mimetypes(), mime):
                out.append(cls)
                outs.add(cls)
    if url:
        # urlparse to remove query parts?
        # try stripping compression extensions?
        # TODO: file patterns could be in leading part of fsspec-like URL
        poss = {}
        for cls in subclasses(BaseData):
            if cls in outs:
                continue
            if cls.filepattern:
                find = re.findall(cls._filepattern(), url.lower())
                if find:
                    poss[cls] = len(find[0])
        out.extend(reversed(sorted(poss, key=lambda x: poss[x])))
    if url:
        for ext in {".gz", ".gzip", ".bzip2", "bz2", ".zstd", ".tar", ".tgz"}:
            if url.endswith(ext):
                out.extend(recommend(url[: -len(ext)], head=False, ignore=outs))
    if out:
        return out

    if head is None and url:
        return recommend(url, mime=mime, head=True, storage_options=storage_options)

    if isinstance(head, bytes):
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
        # TODO: if directory, look inside files?
    return []