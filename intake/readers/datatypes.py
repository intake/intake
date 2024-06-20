"""Enumerates all the sorts of data that Intake knows about"""

from __future__ import annotations

import re
from itertools import chain
from functools import lru_cache as cache
from typing import Any, Optional

import fsspec

from intake import import_name
from intake.readers.utils import Tokenizable, subclasses

# TODO: make "structure" possibilities an enum?


# https://en.wikipedia.org/wiki/List_of_file_signatures


class BaseData(Tokenizable):
    """Prototype dataset definition"""

    mimetypes: str = ""  #: regex, MIME pattern to match
    filepattern: str = ""  #: regex, file URLs to match
    structure: set[str] = set()  #: informational tags for nature of data, e.g., "array"
    magic: set[
        bytes | tuple
    ] = set()  #: binary patterns, usually at the file head; each item identifies this data type
    contains: set[
        str
    ] = set()  #: if using a directory URL, an ls() on that path will contain these things

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

    def to_reader_cls(
        self, outtype: tuple[str] | str | None = None, reader: tuple[str] | str | type | None = None
    ):
        if outtype and reader:
            raise ValueError
        if isinstance(reader, str):
            # exact match (no lowering)
            try:
                return import_name(reader)
            except (ImportError, ModuleNotFoundError):
                reader = (reader,)
        if isinstance(reader, tuple):
            for cls, out in self.possible_outputs.items():
                # there shouldn't be many of these
                if any(re.findall(r.lower(), cls.qname().lower()) for r in reader):
                    return cls
        if isinstance(reader, type):
            return reader
        elif outtype:
            if isinstance(outtype, str):
                outtype = (outtype,)
            for reader, out in self.possible_outputs.items():
                # there shouldn't be many of these
                if out is not None and any(
                    out == _ or re.findall(_.lower(), out.lower()) for _ in outtype
                ):
                    return reader
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
        kw.pop("metadata")  # this is always passed separately
        return DataDescription(datatype=self.qname(), kwargs=kw, metadata=self.metadata)

    def __repr__(self):
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{type(self).__name__}, {d}"

    def auto_pipeline(self, outtype: str | tuple[str]):
        """Find a pipeline to transform from this to the given output type"""
        from intake.readers.convert import auto_pipeline

        return auto_pipeline(self, outtype)


class FileData(BaseData):
    """Datatypes loaded from files, local or remote"""

    def __init__(self, url, storage_options: dict | None = None, metadata: dict | None = None):
        self.url = url  #: location of the file(s), should be str or list[str]
        self.storage_options = storage_options  #: kwargs for a backend storage system
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


class PMTiles(FileData):
    """single-file archive format for tiled image data"""

    filepattern = "pmtiles"
    magic = {b"PMTiles"}
    structure = {"image"}


class DuckDB(FileData):
    """Columnar table DB format used exclusively by duckdb"""

    filepattern = "(duck?)db"
    magic = {(8, b"DUCK")}
    structure = {"table"}


class Parquet(FileData):
    """Column-optimized binary format"""

    filepattern = "(parq|parquet|/$)"
    mimetypes = "application/(vnd.apache.parquet|parquet|x-parquet)/"
    structure = {"table", "nested"}
    magic = {b"PAR1"}
    contains = {"_metadata", "parq", "parquet"}  # a directory can be a dataset


class CSV(FileData):
    """Human-readable tabular format, Comma Separated Values"""

    filepattern = "(csv$|txt$|tsv$)"
    mimetypes = "(text/csv|application/csv|application/vnd.ms-excel)"
    structure = {"table"}


class CSVPattern(CSV):
    """Specialised version of CSV, with a path containing capturing fields

    Characteristically contains python-style format groups with {..}
    """

    filepattern = ".*[{].*[}].*(csv$|txt$|tsv$)"


class Text(FileData):
    """Any text file"""

    filepattern = "(txt$|text$|dat$|ascii$)"
    mimetypes = "text/.*"
    structure = {"sequence"}
    # some optional byte order marks
    # https://en.wikipedia.org/wiki/Byte_order_mark#Byte_order_marks_by_encoding
    magic = {b"\xEF\xBB\xBF", b"\xFE\xFF", b"\xFF\xFE", b"\x00\x00\xFE\xFF"}


class XML(FileData):
    """Extensible Markup Language file"""

    filepattern = "xml[sx]?$"
    mimetypes = "(application|text)/xml"
    structure = {"nested"}
    magic = {b"<?xml "}


class THREDDSCatalog(XML):
    """Datasets on a THREDDS server

    Typically used for "environmental data sources".
    See https://www.unidata.ucar.edu/software/tds/
    """

    magic = {(None, b"<xml.*<catalog ")}
    structure = {"catalog"}


class PNG(FileData):
    """Portable Network Graphics, common image format"""

    filepattern = "png$"
    structure = {"array", "image"}
    mimetypes = "image/png"
    magic = {b"\x89PNG"}


class JPEG(FileData):
    """Image format with good compression for the internet"""

    filepattern = "jpe?g$"
    structure = {"array", "image"}
    mimetypes = "image/jpeg"
    magic = {b"\xFF\xD8\xFF"}


class WAV(FileData):
    """Waveform/sound file"""

    filepattern = "wav$"
    structure = {"array", "timeseries"}
    mimetypes = "audio/wav"
    magic = {(8, b"WAVE")}


class NetCDF3(FileData):
    """Collection of ND-arrays with coordinates, scientific file format"""

    filepattern = "(netcdf3$|nc3?$)"
    structure = {"array"}
    mimetypes = "application/x-netcdf"
    magic = {b"CDF"}


class HDF5(FileData):
    """Hierarchical tree of ND-arrays, widely used scientific file format"""

    filepattern = "(hdf5?|h4|nc4?)$"  # many others by convention
    structure = {"array", "table", "hierarchy"}
    magic = {b"\x89HDF"}
    mimetypes = "application/x-hdf5?"

    def __init__(
        self,
        url,
        storage_options: dict | None = None,
        path: str = "",
        metadata: dict | None = None,
    ):
        """
        path: if given, points to specific array/group in the hierarchy; with "group.subgroup"
            format
        """
        self.url = url
        self.storage_options = storage_options
        self.path = path
        super().__init__(url=url, storage_options=storage_options, metadata=metadata)


class Zarr(FileData):
    """Cloud optimised, chunked N-dimensional file format"""

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
        """
        root: if given, points to specific array/group in the hierarchy
        """
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
    """Text format for sparse array"""

    magic = {b"%%MatrixMarket"}


class Excel(FileData):
    """The well-known spreadsheet app's file format"""

    filepattern = "xls[xmb]?"
    structure = {"tabular"}
    mimetypes = "application/.*(excel|xls)"
    magic = {b"\x50\x4B\x03\x04", b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"}  # will match any office doc


class TIFF(FileData):
    """Image format commonly used for large data"""

    # includes geoTIFF/COG, or split out?
    filepattern = "(tiff?$|cog$)"
    structure = {"array", "image"}
    magic = {b"II*\x00", b"MM\x00*"}
    mimetypes = "image/(geo)?tiff"


class GRIB2(FileData):
    """ "Gridded" file format commonly used in meteo forecasting"""

    filepattern = "gri?b2?$"
    structure = {"array"}
    magic = {b"GRIB"}
    mimetypes = "application/wmo-grib"


class FITS(FileData):
    """Tabular or array data in text/binary format common in astronomy"""

    filepattern = "fits$"  # other conventions too
    structure = {"array", "table"}
    magic = {b"SIMPLE"}
    mimetypes = "(image|application)/fits"


class ASDF(FileData):
    """Advanced Scientific Data Format"""

    filepattern = "asdf$"
    structure = {"array", "table"}
    magic = {b"#ASDF"}


class DICOM(FileData):
    """Imaging data usually from medical scans"""

    filepattern = "(dicom|dcm|ct|mri|DCM)$"  # and others
    structure = {"array", "image"}
    magic = {(128, b"DICM")}
    mimetypes = "application/dicom"


class Nifti(FileData):
    """Medical imaging or volume data file"""

    # https://brainder.org/2012/09/23/the-nifti-file-format/
    filepattern = "(hdr|nii)(\\.gz)?$"
    structure = {"array", "image"}
    magic = {(344, b"\x6E\x69\x31\x00"), (344, b"\x6E\x2B\x31\x00")}


class OpenDAP(Service):
    """Earth-science oriented searchable HTTP API"""

    structure = {"array"}


class SQLQuery(Service):
    """Query on a database-like service"""

    structure = {"sequence", "table"}
    filepattern = "^(oracle|mssql|sqlite|mysql|postgres)"

    def __init__(self, conn, query, metadata=None):
        self.conn = conn
        self.query = query
        super().__init__(metadata)


class Prometheus(Service):
    """Monitoring metric query service"""

    structure = {"structured"}

    def __init__(
        self,
        url,
        options: Optional[dict] = None,
        metric: Optional[str] = None,
        labels: Optional[dict] = None,
        start_time=None,
        end_time=None,
        query: Optional[str] = None,
        metadata=None,
    ):
        if query:
            # this is the totally custom route
            assert metric or labels or start_time or end_time is None
        super().__init__(url, options, metadata)
        self.query = query
        self.metric = metric
        self.labels = labels
        self.start_time = start_time
        self.end_time = end_time


class LlamaCPPService(Service):
    """Simple local HTTP chat

    Also had OpenAI compatible endpoints

    https://github.com/ggerganov/llama.cpp/blob/master/examples/server/README.md
    """

    def open(self):
        """Open chat config and chat page"""
        import webbrowser

        webbrowser.open(self.url)


class OpenAIService(Service):
    """OpenAI compatible chatbot

    See https://platform.openai.com/docs/api-reference/making-requests
    """

    def __init__(
        self,
        url="https://api.openai.com/",
        key: str = "sk-no-key-required",
        options=None,
        metadata=None,
    ):
        self.key = key
        super().__init__(url, options=options, metadata=metadata)


class SQLite(FileData):
    """Database data stored in files"""

    structure = {"sequence", "table"}
    filepattern = "sqlite$|sqlitedb$|db$"
    magic = {b"SQLite format"}


class AVRO(FileData):
    """Structured record passing file format"""

    structure = {"nested"}
    filepattern = "avro$"
    magic = {b"Obj\x01"}
    mimetypes = "avro/binary"


class ORC(FileData):
    """Columnar-optimized tabular binary file format"""

    structure = {"nested", "tabular"}
    filepattern = "orc$"
    magic = {b"ORC"}


class YAMLFile(FileData):
    """Human-readable JSON/object-like format"""

    filepattern = "ya?ml$"
    mimetypes = "text/yaml"
    structure = {"nested"}


class CatalogFile(Catalog, YAMLFile):
    """Intake catalog expressed as YAML"""


class CatalogAPI(Catalog, Service):
    """An API endpoint capable of describing Intake catalogs"""

    filepattern = "^https?:"


class JSONFile(FileData):
    """Nested record format as readable text, very common over HTTP"""

    filepattern = "json[l]$"
    mimetypes = "(text|application)/json"
    structure = {"nested", "table"}
    magic = {b"{"}


class GeoJSON(JSONFile):
    """Geo data (position and geometries) within JSON"""

    filepattern = "(?:geo)?json$"
    magic = {(None, b'"type": "Feature')}  # not guaranteed, but good indicator


class Shapefile(FileData):
    """Geo data (position and geometries) in a set of related binary files"""

    # this would only be found as a member of a .ZIP, since you need all three mandatory
    # files to make a dataset https://en.wikipedia.org/wiki/Shapefile#Overview
    # However, Fiona can read some .shp files with env SHAPE_RESTORE_SHX=YES
    filepattern = "shp$|shx$|dbf$"
    mimetypes = "x-gis/x-shapefile"
    magic = {b"\x00\x00\x27\x0a"}


class FlatGeoBuf(FileData):
    """Geo data in flatbuffers"""

    filepattern = "fgb$"
    magic = {b"fgb"}
    # b"fgb\x03fgb\x01" would be a full magic, encoding version number; here is 3.2


class GeoPackage(SQLite):
    """Geo data (position and geometries) in a SQLite DB file"""

    filepattern = "gpkg$"


class STACJSON(JSONFile):
    """Data assets related to geo data, either as static JSON or a searchable API"""

    magic = {(None, b'"stac_version":')}  # None means "somewhere in the file head"
    mimetypes = "(text|application)/geo\\+json"


class TiledService(CatalogAPI):
    magic = {(None, b"<title>Tiled</title>")}


class TiledDataset(Service):
    """Data access service for data-aware portals and data science tools"""

    structure = {"array", "table", "nested"}


class TileDB(Service):
    """Service exposing versioned, chunked and potentially sparse arrays"""

    filepattern = "tiled://"  # or a real URL, local or remote
    contains = {"__meta", "__schema"}
    structure = {"array", "table"}


class IcebergDataset(JSONFile):
    """Indexed set of parquet files with servioning and diffs"""

    structure = {"tabular"}
    magic = {(None, b'"format-version":')}


class DeltalakeTable(FileData):
    """Indexed set of parquet files with servioning and diffs"""

    # a directory by convention, but otherwise can't be distinguished
    contains = {"_delta_log"}
    filepattern = "/$"
    structure = {"tabular"}


class NumpyFile(FileData):
    """Simple array format"""

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


class Handle(JSONFile):
    """An identifier registered on handle registry

    See https://handle.net/ .

    May refer to a single file or a set of files
    """

    filepattern = "hdl:"


class Feather2(FileData):
    """Tabular format based on Arrow IPC"""

    magic = {b"ARROW1"}
    structure = {"tabular", "nested"}


class Feather1(FileData):
    """Deprecated tabular format from the Arrow project"""

    magic = {b"FEA1"}
    structure = {"tabular", "nested"}


class PythonSourceCode(FileData):
    """Source code file"""

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
    """Tensorflow record file, ready for machine learning"""

    structure = {"nested"}
    filepattern = "tfrec$"


class KerasModel(FileData):
    """Keras model parameter set"""

    structure = {"model"}  # complex
    filepattern = "pb$"  # possibly protobuf


class GGUF(FileData):
    """Trained model

    (see https://github.com/ggerganov/ggml/blob/master/docs/gguf.md)"""

    structure = {"model"}
    filepattern = "gguf$"
    magic = {b"GGUF"}


class SafeTensors(FileData):
    """Trained model

    (see https://github.com/huggingface/safetensors?tab=readme-ov-file#format)
    """

    # TODO: .bin sees to be an older pytorch-specific version of this
    structure = {"model"}
    filepattern = "safetensors$"
    magic = {(8, b"{")}


class PickleFile(FileData):
    """Python pickle, arbitrary serialized object"""

    structure = set()


class ModelConfig(FileData):
    """HuggingFace-style multi-file model directory

    Looks like a catalog of related models
    """

    structure = {"model"}
    filepattern = "config.json"
    magic = {b'"model_type":'}


class SKLearnPickleModel(PickleFile):
    """Trained model made by sklearn and saved as pickle"""


comp_magic = {
    # These are a bit like datatypes making raw bytes/file object output
    (0, b"\x1f\x8b"): "gzip",
    (0, b"BZh"): "bzip2",
    (0, b"(\xc2\xb5/\xc3\xbd"): "zstd",
    (0, b"\xff\x06\x00\x00sNaPpY"): "sz",  # stream framed format
}
container_magic = {
    # these are like datatypes making filesystems
    (257, b"ustar"): "tar",
    (0, b"PK"): "zip",
}


def recommend(
    url: str | None = None,
    mime: str | None = None,
    head: bool = True,
    contents: bool = False,
    storage_options=None,
    ignore: set[str] | None = None,
) -> set[BaseData]:
    """Show which Intake data types can apply to the given details

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
    contents: bool | None
        Attempt to delve into URL to analyse constituent files. This can significantly slow
        your recommendation.
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
            fs, url2 = fsspec.core.url_to_fs(url, **(storage_options or {}))
            head = fs.cat_file(url2[0] if isinstance(url2, list) else url2, end=2**20)
        except (IOError, IndexError, ValueError):
            head = False
    else:
        fs = None

    if isinstance(head, bytes):
        # more specific first
        for cls in subclasses(BaseData):
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
        mime = mime.lower()
        for cls in subclasses(BaseData):
            if cls not in outs and cls.mimetypes and re.match(cls._mimetypes(), mime):
                out.append(cls)
                outs.add(cls)
    if url:
        poss = {}
        if fs is not None and contents:
            try:
                allfiles = fs.ls(url, detail=False)
            except IOError:
                allfiles = None
        else:
            allfiles = None
        files = set(subclasses(FileData))
        bases = set(subclasses(BaseData)) - files
        # file types first, then other/services, more specific first
        for cls in chain(files, bases):
            if cls in outs:
                continue
            if cls.filepattern:
                find = re.search(cls._filepattern(), url.lower())
                if find is None and getattr(cls, "contains", None) and allfiles:
                    if any(a.endswith(c) for c in cls.contains for a in allfiles):
                        poss[cls] = 0
                if find:
                    poss[cls] = find.start()
        out.extend(sorted(poss, key=lambda x: poss[x]))
    if contents and url:
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
