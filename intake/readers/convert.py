"""Convert between python representations of data

By convention, functions here do not change the data, just how it is held.
"""
from __future__ import annotations

import copy
import re
from urllib.parse import urljoin
from functools import lru_cache
from itertools import chain

from intake import import_name, conf
from intake.readers.datatypes import OpenAIService
from intake.readers import BaseData, BaseReader, readers, LlamaServerReader, OpenAIReader
from intake.readers.utils import all_to_one, subclasses, safe_dict


class ImportsProperty:
    """Derives ``.imports`` from the package names in ``.instances``.

    Set as a class-level descriptor on :class:`BaseConverter` so that every
    subclass automatically has the right ``imports`` without needing to repeat
    them manually.  The first access replaces the descriptor on the concrete
    class with a plain ``set`` (caching), so there is no per-call overhead
    after the first lookup.

    Subclasses that need packages *beyond* what appears in ``instances``
    (e.g. an output converter that always needs ``soundfile`` but whose
    ``instances`` only mentions ``numpy`` and an intake datatype) can still
    override ``imports`` with an explicit ``set``.
    """

    def __get__(self, obj, cls):
        # Walk the MRO so that a subclass that hasn't overridden `instances`
        # still sees the right packages.
        instances = cls.instances
        derived = set(
            _.split(":", 1)[0].split(".", 1)[0]
            for _ in chain(instances, instances.values())
            if _ is not SameType and not _.startswith("intake.")
        )
        # Cache on the concrete class so subsequent accesses are O(1)
        cls.imports = derived
        return derived


class BaseConverter(BaseReader):
    """Converts from one object type to another

    Most often, subclasses call a single function on the data, but arbitrary
    complex transforms are possible.  This is designed to be one step in a
    :class:`~intake.readers.convert.Pipeline`.

    Subclasses should set:

    ``instances``
        A ``{input_type_qname: output_type_qname}`` mapping.  Keys and values
        are ``"module:Class"`` strings matching ``output_instance`` on readers.

    ``func``
        The primary callable as a ``"module:name"`` string.  Used by
        :meth:`doc` and :attr:`_func` (inherited from
        :class:`~intake.readers.readers.BaseReader`).  Subclasses that perform
        more than one function call should still set ``func`` to the *main*
        entry-point for documentation purposes, and override :meth:`run`.

    ``is_ok``
        Override to reject in-memory objects that this converter cannot handle
        even when the type name matches (e.g. wrong ``ndim`` or ``dtype``).
    """

    instances: dict[str, str] = {}  #: mapping from input types to output types
    imports = ImportsProperty()  #: derived automatically from ``instances``

    @classmethod
    def is_ok(cls, x) -> bool:
        """Return ``True`` if this converter can handle the concrete object *x*.

        This is the converter analogue of :meth:`BaseReader.is_ok`.  The
        default implementation always returns ``True``; subclasses override it
        to enforce constraints on the in-memory object (e.g. ``ndim``,
        ``dtype``, shape) that go beyond the type-name match in ``instances``.

        Parameters
        ----------
        x:
            The concrete in-memory object that would be passed to :meth:`run`.

        Returns
        -------
        bool
            ``False`` to exclude this converter from :func:`convert_classes`
            results for this particular object.
        """
        return True

    @classmethod
    def doc(cls):
        """Documentation for this conversion step.

        Mirrors :meth:`BaseReader.doc <intake.readers.readers.BaseReader.doc>`
        so that converters participate in the same help/introspection
        conventions as readers.
        """
        import inspect as _inspect

        f = cls.func_doc or cls.func
        if isinstance(f, str) and f != "builtins:NotImplementedError":
            try:
                f = import_name(f)
                upstream = f.__doc__ or ""
            except Exception:
                upstream = ""
        else:
            upstream = ""
        sig = str(_inspect.signature(cls.run))
        doc = cls.run.__doc__
        return "\n\n".join(_ for _ in [cls.qname(), cls.__doc__, sig, doc, upstream] if _)

    def run(self, x, *args, **kwargs):
        """Execute a conversion stage on the output object from another stage

        Subclasses may override this
        """
        func = import_name(self.func)
        return func(x, *args, **kwargs)

    def _read(self, *args, data=None, **kwargs):
        """Read the data

        Subclasses may override this if they wish to interact with the upstream reader/pipeline.
        """
        if data is None:
            data = args[0]
            args = args[1:]
        if isinstance(data, BaseReader):
            data = data.read()
        return self.run(data, *args, **kwargs)


class GenericFunc(BaseConverter):
    """Call given arbitrary function

    This could be a transform or anything; the caller should specify what the
    output_instance will be, since the class doesn't know.
    """

    def _read(self, *args, data=None, func=None, data_kwarg=None, **kwargs):
        if data is not None and isinstance(data, BaseReader):
            data = data.read()
        if data is not None:
            if data_kwarg is None:
                return func(data, *args, **kwargs)
            else:
                kwargs[data_kwarg] = data
                return func(*args, **kwargs)
        return func(*args, **kwargs)


class SameType:
    """Used to indicate that the output of a transform is the same as the input, which is arbitrary"""


class DuckToPandas(BaseConverter):
    instances = {"duckdb:DuckDBPyRelation": "pandas:DataFrame"}
    func = "duckdb:DuckDBPyConnection.df"

    def run(self, x, *args, with_arrow=True, **kwargs):
        if with_arrow:
            import pandas as pd

            table = x.to_arrow_table()
            data = {
                col: pd.Series(pd.arrays.ArrowExtensionArray(val))
                for col, val in zip(table.column_names, table.columns)
            }
            return pd.DataFrame(data)
        else:
            return x.df()


class PandasToDuck(BaseConverter):
    """Save content of pandas dataframe to Duck internal storage

    Value of ``table`` can be used to point to attached database or
    output file.
    """

    instances = {"pandas:DataFrame": "duckdb:DuckDBPyRelation"}
    func = "duckdb:df"

    def run(
        self,
        x,
        table: str,
        conn: dict | str | None = None,
        *args,
        comment: str = None,
        overwrite=True,
        **kwargs,
    ):
        # TODO: more options like metadata
        import duckdb
        from intake.readers.datatypes import SQLQuery

        duck = readers.DuckSQL._duck(None, conn=conn)
        out = duckdb.df(x, connection=duck)
        duck.register(view_name="temp_view", python_object=out)
        duck.sql(
            f"CREATE {'OR REPLACE' if overwrite else ''} "
            f"TABLE '{table}' AS SELECT * FROM 'temp_view';"
        )
        if comment is not None:
            # https://duckdb.org/docs/stable/sql/data_types/
            #   literal_types.html#escape-string-literals
            comment = str(comment).replace("'", "''")
            duck.sql(f"COMMENT ON TABLE '{table}' IS '{comment}';")
        out = readers.DuckSQL(SQLQuery(conn=conn, query=f"SELECT * FROM '{table}'"))
        return out


class DaskDFToPandas(BaseConverter):
    instances = {
        "dask.dataframe:DataFrame": "pandas:DataFrame",
        "dask_geopandas.core:GeoDataFrame": "geopandas:GeoDataFrame",
        "dask.array:Array": "numpy:ndarray",
    }
    func = "dask:compute"

    def run(self, x, *args, **kwargs):
        return self._func(x)[0]


class PandasToGeopandas(BaseConverter):
    instances = {"pandas:DataFrame": "geopandas:GeoDataFrame"}
    func = "geopandas:GeoDataFrame"


class XarrayToPandas(BaseConverter):
    instances = {"xarray:Dataset": "pandas:DataFrame"}
    func = "xarray:Dataset.to_dataframe"


class PandasToXarray(BaseConverter):
    instances = {"pandas:DataFrame": "xarray:Dataset"}
    func = "xarray:Dataset.from_dataframe"


class ToHvPlot(BaseConverter):
    instances = all_to_one(
        {
            "pandas:DataFrame",
            "dask.dataframe:DataFrame",
            "xarray:Dataset",
            "xarray:DataArray",
        },
        "holoviews.core.layout:Composable",
    )
    func = "hvplot:hvPlot"

    def run(self, data, explorer: bool = False, **kw):
        """For tabular data only, pass explorer=True to get an interactive GUI"""
        import hvplot

        if explorer:
            # this is actually a hvplot.ui:hvPlotExplorer and only allows tabular data
            return hvplot.explorer(data, **kw)
        return hvplot.hvPlot(data, **kw)()


class RayToPandas(BaseConverter):
    instances = {"ray.data:Dataset": "pandas:DataFrame"}
    func = "ray.data:Dataset.to_pandas"


class PandasToRay(BaseConverter):
    instances = {"pandas:DataFrame": "ray.data:Dataset"}
    func = "ray.data:from_pandas"


class RayToDask(BaseConverter):
    instances = {"ray.data:Dataset": "dask.dataframe:DataFrame"}
    func = "ray.data:Dataset.to_dask"


class DaskToRay(BaseConverter):
    instances = {"dask.dataframe:DataFrame": "ray.data:Dataset"}
    func = "ray.data:from_dask"


class HuggingfaceToRay(BaseConverter):
    instances = {"datasets.arrow_dataset:Dataset": "ray.data:Dataset"}
    func = "ray.data:from_huggingface"


class TorchToRay(BaseConverter):
    instances = {"torch.utils.data:Dataset": "ray.data:Dataset"}
    func = "ray.data:from_torch"


class SparkDFToRay(BaseConverter):
    instances = {"pyspark.sql:DataFrame": "ray.data:Dataset"}
    func = "ray.data:from_spark"


class RayToSpark(BaseConverter):
    instances = {"ray.data:Dataset": "pyspark.sql:DataFrame"}
    func = "ray.data:Dataset.to_spark"


class TiledNodeToCatalog(BaseConverter):
    instances = {"tiled.client.node:Node": "intake.readers.entry:Catalog"}
    func = "tiled.client.node:Node.items"

    def run(self, x, **kw):
        # eager creation of entries from a node
        from intake.readers.datatypes import TiledDataset, TiledService
        from intake.readers.entry import Catalog
        from intake.readers.readers import TiledClient, TiledNode

        cat = Catalog()
        for k, client in x.items():
            if type(client).__name__ == "Node":
                data = TiledService(url=client.uri)
                reader = TiledNode(data=data, metadata=client.item)
                cat[k] = reader
            else:
                data = TiledDataset(url=client.uri)
                reader = TiledClient(
                    data,
                    output_instance=f"{type(client).__module__}:{type(client).__name__}",
                    metadata=client.item,
                )
                cat[k] = reader
        return cat


class TiledSearch(BaseConverter):
    """See https://blueskyproject.io/tiled/tutorials/search.html"""

    instances = {"tiled.client.node:Node": "tiled.client.node:Node"}
    func = "tiled.client.node:Node.search"

    def run(self, x, *arg, **kw):
        # TODO: expects instances of classes in tiled.queries, which must be pickled, but
        #  could allow (name, args) or something else
        return x.search(*arg, **kw)


class TileDBToNumpy(BaseConverter):
    instances = {"tiledb.libtiledb:Array": "numpy:ndarray"}
    func = "tiledb.libtiledb:Array.__getitem__"

    def run(self, x, *args, **kwargs):
        # allow attribute selection here for when it wasn't included at read time?
        return x[:]


class TileDBToPandas(BaseConverter):
    """Implemented only if an attribute was not already chosen."""

    instances = {"tiledb.libtiledb:Array": "pandas:DataFrame"}
    func = "tiledb.libtiledb:Array.df"

    def run(self, x, *args, **kwargs):
        return x.df[:]


class DaskArrayToTileDB(BaseConverter):
    # this is like output, and could return a datatypes.TileDB instead
    instances = {"dask.array:Array": "tiledb.libtiledb:Array"}
    func = "dask.array:to_tiledb"

    def run(self, x, uri, **kwargs):
        return self._func(x, uri, return_stored=True, **kwargs)


class NumpyToTileDB(BaseConverter):
    # this could be considered an output converter, giving a datatypes.TileDB
    # instead of the array instance
    # alternatively, a datatypes.TileDB could be the *input* to the function
    instances = {"numpy:ndarray": "tiledb.libtiledb:Array"}
    func = "tiledb:from_numpy"

    def run(self, x, uri, **kwargs):
        return self._func(uri, x, **kwargs)


class DeltaQueryToDask(BaseConverter):
    instances = {"deltalake:DeltaTable": "dask.dataframe:DataFrame"}
    func = "deltalake:DeltaTable.file_uris"

    def _read(self, reader, query, *args, **kwargs):
        import dask.dataframe as dd

        file_uris = reader.read().file_uris(query)

        return dd.read_parquet(file_uris, storage_options=reader.kwargs["data"].storage_options)


class DeltaQueryToDaskGeopandas(BaseConverter):
    instances = {"deltalake:DeltaTable": "dask_geopandas:GeoDataFrame"}
    func = "deltalake:DeltaTable.file_uris"

    def _read(self, reader, query, *args, **kwargs):
        import dask_geopandas

        file_uris = reader.read().file_uris(query)

        return dask_geopandas.read_parquet(
            file_uris, storage_options=reader.kwargs["data"].storage_options
        )


class GeoDataFrameToSTACCatalog(BaseConverter):
    instances = {"geopandas:GeoDataFrame": "intake.readers.entry:Catalog"}
    func = "intake.readers.catalogs:StacCatalogReader"

    @classmethod
    def _un_arr(cls, data):
        # clean up dataframe
        import numpy as np

        if isinstance(data, dict):
            data = {k: cls._un_arr(v) for k, v in data.items()}
        elif isinstance(data, (list, np.ndarray)):
            data = [cls._un_arr(_) for _ in data]
        return data

    def read(self, data, *args, **kwargs):
        from intake.readers import Literal
        from intake.readers.catalogs import StacCatalogReader
        import stac_geoparquet

        # clean up numpy arrays->list and any assets that are just None
        data["assets"] = data.assets.apply(
            lambda x: {k: v for k, v in self._un_arr(x).items() if v}
        )
        stac = stac_geoparquet.stac_geoparquet.to_item_collection(data)
        lit = Literal(stac.to_dict())
        return StacCatalogReader(
            lit,
            signer=self.metadata.get("signer"),
            prefer=self.metadata.get("prefer"),
            cls="ItemCollection",
            metadata=self.metadata,
        ).read()


class PandasToMetagraph(BaseConverter):
    instances = {"pd:DataFrame": "metagraph.wrappers.EdgeSet:PandasEdgeSet"}
    func = "metagraph.wrappers.EdgeSet:PandasEdgeSet"


class NibabelToNumpy(BaseConverter):
    instances = {"nibabel.spatialimages:SpatialImage": "numpy:ndimage"}
    func = "nibabel.spatialimages:SpatialImage.get_fdata"


class DicomToNumpy(BaseConverter):
    instances = {"pydicom.dataset:FileDataset": "numpy:ndarray"}
    func = "pydicom.dataset:FileDataset.pixel_array"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Only applicable when the DICOM file contains pixel data."""
        return hasattr(x, "PixelData")

    def run(self, x, *args, **kwargs):
        return x.pixel_array


class FITSToNumpy(BaseConverter):
    instances = {"astropy.io.fits:HDUList": "numpy:ndarray"}
    func = "astropy.io.fits:FitsHDU.data"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Only applicable when the HDUList contains at least one data extension."""
        return any(hdu.header.get("NAXIS", 0) > 0 for hdu in x)

    def run(self, x, extension=None):
        """Get the array data of one FITS extension

        If hdu is None, find first extension containing data.
        """
        if extension is None:
            found = False
            for extension, hdu in enumerate(x):
                if hdu.header.get("NAXIS", 0) > 0:
                    found = True
                    break
            if not found:
                raise ValueError("No data extensions")
        return x[extension].data


class ASDFToNumpy(BaseConverter):
    instances = {"asdf:AsdfFile": "numpy:ndarray"}
    func = "asdf:AsdfFile.__getitem__"

    def run(self, x, tree_path: str | list[str], **kwargs):
        if isinstance(tree_path, str):
            tree_path = tree_path.split(".")
        for p in tree_path:
            x = x[p]
        return x[:]


class PolarsLazy(BaseConverter):
    instances = {"polars:DataFrame": "polars:LazyFrame"}
    func = "polars:DataFrame.lazy"


class PolarsEager(BaseConverter):
    instances = {"polars:LazyFrame": "polars:DataFrame"}
    func = "polars:LazyFrame.collect"  # collect_async() ?


class PolarsToPandas(BaseConverter):
    instances = {"polars:DataFrame": "pandas:DataFrame"}
    func = "polars:DataFrame.to_pandas"

    def run(self, x, *args, **kwargs):
        return x.to_pandas(*args, **kwargs)


class PandasToPolars(BaseConverter):
    instances = {"pandas:DataFrame": "polars:DataFrame"}
    func = "polars:from_pandas"


class DataFrameToMetadata(BaseConverter):
    func = "builtins:repr"  # primary operation used for all branches
    instances = all_to_one(
        ["pandas:DataFrame", "dask.dataframe:DataFrame", "polars:DataFrame"], "builtins:dict"
    )

    def run(self, x, *args, **kwargs):
        out = {"repr": repr(x), "shape": x.shape}  # cf Repr, the output converter
        t = str(type(x)).lower()
        # TODO: perhaps can split this class into several
        # TODO: implement spark, daft, modin, ibis ...
        # Note that FileSizeReader can give file size on disk (if origin is files)
        if "pandas" in t:
            out["memory"] = x.memory_usage(deep=True).sum()
            out["schema"] = x.dtypes if hasattr(x, "dtypes") else x.dtype
            out["shape"] = x.shape
        elif "polars" in t:
            out["memory"] = x.estimated_size()
            out["shape"] = x.shape
            out["schema"] = x.schema
        elif "ray" in t:
            out["memory"] = x.size_bytes()
            out["shape"] = [x.count(), len(x.columns)]
            out["schema"] = safe_dict(x.schema)
        return safe_dict(out)


class GGUFToLlamaCPPService(BaseConverter):
    instances = {"intake.readers.datatypes:GGUF": "intake.readers.datatypes:LlamaCPPService"}
    func = "intake.readers.readers:LlamaServerReader.read"

    def run(self, x, **kwargs):
        return LlamaServerReader(x).read(**kwargs)


class LLamaCPPServiceToOpenAIService(BaseConverter):
    instances = {
        "intake.readers.datatypes:LlamaCPPService": "intake.readers.datatypes:OpenAIService"
    }
    func = "intake.readers.datatypes:OpenAIService"

    def run(self, x, options=None):
        url = urljoin(x.url, "/v1")
        service = OpenAIService(url=url, key="none", options=options)
        return service


class OpenAIServiceToOpenAIClient(BaseConverter):
    instances = {"intake.readers.datatypes:OpenAIService": "openai:OpenAI"}
    func = "intake.readers.readers:OpenAIReader.read"

    def run(self, x):
        return OpenAIReader(x).read()


# ---------------------------------------------------------------------------
# Image / media conversions
# ---------------------------------------------------------------------------


class NumpyToPIL(BaseConverter):
    """Convert a NumPy array to a PIL Image.

    Works for 2-D (grayscale), 3-D (H×W×C RGB/RGBA) and uint8/float arrays.
    Float arrays in [0, 1] are scaled to uint8 before conversion.
    """

    instances = {"numpy:ndarray": "PIL.Image:Image"}
    func = "PIL.Image:fromarray"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a 2-D or 3-D numeric array (grayscale, RGB, or RGBA)."""
        return getattr(x, "ndim", 0) in (2, 3) and getattr(x, "dtype", None) is not None

    def run(self, x, mode=None, **kwargs):
        from PIL import Image
        import numpy as np

        if x.dtype.kind == "f":
            x = (np.clip(x, 0.0, 1.0) * 255).astype(np.uint8)
        return Image.fromarray(x, mode=mode)


class PILToNumpy(BaseConverter):
    """Convert a PIL Image to a NumPy array."""

    instances = {"PIL.Image:Image": "numpy:ndarray"}
    func = "numpy:asarray"

    def run(self, x, **kwargs):
        import numpy as np

        return np.asarray(x)


class SimpleITKToNumpy(BaseConverter):
    """Extract the pixel data from a SimpleITK Image as a NumPy array.

    The returned array has axes ordered as ``(z, y, x)`` (or ``(y, x)`` for 2-D
    images), matching the ITK convention.
    """

    instances = {"SimpleITK:Image": "numpy:ndarray"}
    func = "SimpleITK:GetArrayFromImage"

    def run(self, x, **kwargs):
        import SimpleITK as sitk

        return sitk.GetArrayFromImage(x)


class NumpyToSimpleITK(BaseConverter):
    """Wrap a NumPy array in a SimpleITK Image.

    Optionally supply ``spacing``, ``origin``, and ``direction`` as keyword
    arguments to preserve physical-space metadata.
    """

    instances = {"numpy:ndarray": "SimpleITK:Image"}
    func = "SimpleITK:GetImageFromArray"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a numeric (integer or float) array."""
        return getattr(getattr(x, "dtype", None), "kind", None) in ("u", "i", "f")

    def run(self, x, spacing=None, origin=None, direction=None, **kwargs):
        import SimpleITK as sitk

        img = sitk.GetImageFromArray(x)
        if spacing is not None:
            img.SetSpacing(spacing)
        if origin is not None:
            img.SetOrigin(origin)
        if direction is not None:
            img.SetDirection(direction)
        return img


class SimpleITKToXarray(BaseConverter):
    """Convert a SimpleITK Image to an xarray DataArray with physical coordinates.

    Spacing and origin from the ITK image are used to build labelled coordinate
    axes (``z``, ``y``, ``x`` or ``y``, ``x`` for 2-D images).
    """

    instances = {"SimpleITK:Image": "xarray:DataArray"}
    func = "SimpleITK:GetArrayFromImage"

    def run(self, x, **kwargs):
        import SimpleITK as sitk
        import xarray as xr
        import numpy as np

        arr = sitk.GetArrayFromImage(x)
        ndim = arr.ndim
        spacing = x.GetSpacing()  # (x, y[, z]) order
        origin = x.GetOrigin()  # (x, y[, z]) order

        # ITK spacing/origin are in x-first order; array axes are reversed (z first)
        dim_names = ["z", "y", "x"][-ndim:]
        coords = {
            name: origin[ndim - 1 - i] + np.arange(arr.shape[i]) * spacing[ndim - 1 - i]
            for i, name in enumerate(dim_names)
        }
        return xr.DataArray(arr, dims=dim_names, coords=coords)


class OpenSlideToNumpy(BaseConverter):
    """Read a region from an OpenSlide whole-slide image into a NumPy array (RGBA).

    By default reads the entire slide at ``level=0``.  For large slides,
    supply ``level``, ``location`` (top-left corner in level-0 pixels), and
    ``size`` (width, height) to read a sub-region.
    """

    instances = {"openslide:OpenSlide": "numpy:ndarray"}
    func = "openslide:OpenSlide.read_region"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require an open slide with at least one pyramid level."""
        return getattr(x, "level_count", 0) >= 1

    def run(self, x, level=0, location=None, size=None, **kwargs):
        import numpy as np

        if size is None:
            size = x.level_dimensions[level]
        if location is None:
            location = (0, 0)
        region = x.read_region(location, level, size)
        return np.asarray(region)


class OpenSlideToPIL(BaseConverter):
    """Read a region from an OpenSlide whole-slide image as a PIL Image (RGBA).

    By default reads the entire slide at ``level=0``.
    """

    instances = {"openslide:OpenSlide": "PIL.Image:Image"}
    func = "openslide:OpenSlide.read_region"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require an open slide with at least one pyramid level."""
        return getattr(x, "level_count", 0) >= 1

    def run(self, x, level=0, location=None, size=None, **kwargs):
        if size is None:
            size = x.level_dimensions[level]
        if location is None:
            location = (0, 0)
        return x.read_region(location, level, size)


class DecordToNumpy(BaseConverter):
    """Read all (or a subset of) frames from a decord VideoReader into a NumPy array.

    Returns an array of shape ``(n_frames, height, width, channels)``.
    Supply ``indices`` (list of ints) to select specific frames; omit it to
    read every frame.
    """

    instances = {"decord:VideoReader": "numpy:ndarray"}
    func = "decord:VideoReader.get_batch"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a non-empty VideoReader."""
        try:
            return len(x) > 0
        except Exception:
            return False

    def run(self, x, indices=None, **kwargs):
        import numpy as np

        if indices is None:
            indices = list(range(len(x)))
        frames = x.get_batch(indices)
        # decord returns an NDArray; convert to plain numpy
        return np.asarray(frames)


class ImageIOVideoToNumpy(BaseConverter):
    """Collect all frames from an imageio video Reader into a NumPy array.

    Returns an array of shape ``(n_frames, height, width, channels)``.
    """

    instances = {"imageio.core.format:Reader": "numpy:ndarray"}
    func = "imageio.core.format:Reader.get_data"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a Reader with at least one frame."""
        try:
            return len(x) > 0
        except Exception:
            return True  # some readers don't support len(); allow them through

    def run(self, x, **kwargs):
        import numpy as np

        return np.stack([np.asarray(frame) for frame in x], axis=0)


def convert_class(data, out_type: str):
    """Get conversion class from given data to out_type

    This works on concrete data, not a datatype or reader instance. It returns the
    first match. out_type will match on regex, e.g., "pandas" would match "pandas:DataFrame"
    """
    package = type(data).__module__.split(".", 1)[0]
    for cls in subclasses(BaseConverter):
        for intype, outtype in cls.instances.items():
            if not re.findall(out_type, outtype):
                continue
            if intype.split(".", 1)[0] != package:
                continue
            thing = readers.import_name(intype)
            if isinstance(data, thing):
                if not cls.is_ok(data):
                    continue
                return cls
    raise ValueError("Converter not found")


def convert_classes(in_type: str, obj=None):
    """Get available conversion classes for input type

    Parameters
    ----------
    in_type:
        Qualified type name of the in-memory object (e.g. ``"numpy:ndarray"``).
    obj:
        Optional concrete object.  When supplied, :meth:`BaseConverter.is_ok`
        is called on each candidate converter so that converters whose
        constraints are not met (e.g. wrong ``ndim`` or ``dtype``) are
        excluded from the returned list.
    """
    out_dict = []
    package = in_type.split(":", 1)[0].split(".", 1)[0]
    for cls in subclasses(BaseConverter):
        for intype, outtype in cls.instances.items():
            if "*" not in intype and intype.split(":", 1)[0].split(".", 1)[0] != package:
                continue
            if re.findall(intype.lower(), in_type.lower()) or re.findall(
                in_type.lower(), intype.lower()
            ):
                if obj is not None and not cls.is_ok(obj):
                    continue
                if outtype == SameType:
                    outtype = intype
                out_dict.append((outtype, cls))
    return out_dict


class Pipeline(readers.BaseReader):
    """Holds a list of transforms/conversions to be enacted in sequence

    A transform on a pipeline makes a new pipeline with that transform added to the sequence
    of operations.
    """

    from intake.readers.readers import BaseReader

    def __init__(
        self,
        steps: list[tuple[BaseReader, tuple, dict]],
        out_instances: list[str],
        output_instance=None,
        metadata=None,
        **kwargs,
    ):
        self.output_instances = []
        prev = out_instances[0]
        for inst in out_instances:
            if inst is SameType:
                inst = prev
            prev = inst
            self.output_instances.append(inst)
        super().__init__(
            output_instance=output_instance or self.output_instances[-1],
            metadata=metadata,
            steps=steps,
            out_instances=self.output_instances,
        )
        steps[-1][2].update(kwargs)

    @property
    def steps(self):
        return self.kwargs["steps"]

    def __call__(self, *args, **kwargs):
        return super().__call__(
            *args,
            steps=self.steps,
            out_instances=self.output_instances,
            metadata=self.metadata,
            **kwargs,
        )

    def __repr__(self):
        start = "PipelineReader: \n"
        bits = [
            f"  {i}: {f.qname() if isinstance(f, BaseReader) else f.__name__}, {args} {kw} => {out}"
            for i, ((f, args, kw), out) in enumerate(zip(self.steps, self.output_instances))
        ]
        return "\n".join([start] + bits)

    def output_doc(self):
        from intake import import_name

        out = import_name(self.output_instance)
        return out.__doc__

    def doc(self):
        return self.doc_n(-1)

    def doc_n(self, n):
        """Documentation for the Nth step"""
        return self.steps[n][0].doc()

    def _read_stage_n(self, stage, discover=False, **kwargs):
        from intake.readers.readers import BaseReader

        func, arg, kw = self.steps[stage]

        kw2 = kw.copy()
        kw2.update(kwargs)
        for k, v in kw.items():
            if isinstance(v, BaseReader):
                kw2[k] = v.read()
            else:
                kw2[k] = v
        arg = kw2.pop("args", arg)
        # TODO: these conditions can probably be combined
        if isinstance(func, type) and issubclass(func, BaseReader):
            if discover:
                from intake.readers.convert import BaseConverter

                if issubclass(func, BaseConverter):
                    # Converter steps transform data — run them normally even
                    # during discover (they operate on the already-sampled data).
                    return func(metadata=self.metadata).read(*arg, **kw2)
                else:
                    return func(metadata=self.metadata).discover(**kw2)
            else:
                return func(metadata=self.metadata).read(*arg, **kw2)
        elif isinstance(func, BaseReader):
            if discover:
                from intake.readers.convert import BaseConverter

                if isinstance(func, BaseConverter):
                    return func.read(*arg, **kw2)
                else:
                    return func.discover(**kw2)
            else:
                return func.read(*arg, **kw2)
        else:
            return func(*arg, **kw2)

    def _read(self, discover=False, **kwargs):
        data = None
        for i, step in enumerate(self.steps):
            kw = kwargs if i == len(self.steps) else {}
            if i:
                data = self._read_stage_n(i, data=data, discover=discover, **kw)
            else:
                data = self._read_stage_n(i, discover=discover, **kw)
        return data

    def apply(self, func, *arg, output_instance=None, **kwargs):
        """Add a pipeline stage applying function to the pipeline output so far"""
        from intake.readers.convert import GenericFunc

        kwargs["func"] = func
        return self.with_step((GenericFunc, arg, kwargs), output_instance or self.output_instance)

    def first_n_stages(self, n: int):
        """Truncate pipeline to the given stage

        If n is equal to the number of steps, this is a simple copy.
        """
        # TODO: allow n=0 to get the basic reader?
        if n < 1 or n > len(self.steps):
            raise ValueError(f"n must be between {1} and {len(self.steps)}")

        kw = self.kwargs.copy()
        kw.update(
            dict(
                steps=self.steps[:n],
                out_instances=self.output_instances[:n],
                metadata=self.metadata,
            )
        )
        pipe = Pipeline(
            **kw,
        )
        if n < len(self.steps):
            pipe._tok = (self.token, n)
        return pipe

    def discover(self, **kwargs):
        return self.read(discover=True)

    def with_step(self, step, out_instance):
        """A new pipeline like this one but with one more step"""
        if not isinstance(step, tuple):
            # must be a func - check?
            step = (step, (), {})
        return Pipeline(
            steps=self.steps + [step],
            out_instances=self.output_instances + [out_instance],
            metadata=self.metadata,
        )

    def read_stepwise(self, breakpoint=0):
        """Read with a wrapper class to allow executing one step at a time

        Parameters
        ----------
        breakpoint: int
            At which stage of the pipeline to enter stepwise mode
        """
        return PipelineExecution(self, breakpoint=breakpoint)


class PipelineExecution:

    """Encapsulates a Pipeline, so you can step through it stepwise

    Interesting attributes to examine:
    - .data, the result of the most recent step (initially None)
    - .next, (i, step) the next step to perform. This is a copy, so
      you can edit the kwargs in-place without changing the original
    """

    def __init__(self, pipeline, breakpoint=0):
        self.pipeline = pipeline
        self.data = None
        self.steps = iter(enumerate(pipeline.steps))
        self.next = copy.copy(next(self.steps))
        for _ in range(breakpoint):
            self.step()

    def __repr__(self):
        return f"Executing stage {self.next[0]} of pipeline\n{self.pipeline}"

    def cont(self):
        """Continue pipeline to the end without stopping again"""
        while True:
            out = self.step()
            if out is not self:
                return out

    def step(self, **kw):
        """Run one step of the pipeline

        If it is the last step, will return the result; otherwise
        will return self.
        """
        i, step = self.next
        if i:
            self.data = self.pipeline._read_stage_n(i, data=self.data, **kw)
        else:
            self.data = self.pipeline._read_stage_n(i, **kw)
        try:
            self.next = next(self.steps)
            return self
        except StopIteration:
            return self.data


def conversions_graph(avoid=None, allow_wildcard=True):
    avoid = avoid or conf["reader_avoid"]
    if isinstance(avoid, str):
        avoid = [avoid]
    import networkx

    graph = networkx.DiGraph()

    # transformers
    nodes = set(
        cls.output_instance
        for cls in subclasses(readers.BaseReader)
        if cls.output_instance
        and not any(re.findall(_.lower(), cls.qname().lower()) for _ in avoid)
    )
    graph.add_nodes_from(nodes)

    for cls in subclasses(readers.BaseReader):
        if any(re.findall(_.lower(), cls.qname().lower()) for _ in avoid):
            continue
        if cls.output_instance:
            for impl in cls.implements:
                graph.add_node(cls.output_instance)
                graph.add_edge(impl.qname(), cls.output_instance, label=cls.qname())
    for cls in subclasses(BaseConverter):
        if any(re.findall(_.lower(), cls.qname().lower()) for _ in avoid):
            continue
        for inttype, outtype in cls.instances.items():
            if (
                isinstance(outtype, str)
                and inttype != outtype
                and (allow_wildcard or "*" not in inttype)
            ):
                graph.add_nodes_from((inttype, outtype))
                graph.add_edge(inttype, outtype, label=cls.qname())

    return graph


def plot_conversion_graph(filename) -> None:
    # TODO: return a PNG datatype or something else?
    import networkx as nx

    g = conversions_graph(allow_wildcard=False)
    a = nx.nx_agraph.to_agraph(g)  # requires pygraphviz
    a.draw(filename, prog="fdp")


@lru_cache()  # clear cache if you import more things
def path(
    start: str, end: str | tuple[str], cutoff: int = 5, avoid: tuple[str] | None = None
) -> list[list]:
    """Find possible conversion paths from start to end types

    Parameters
    ----------
    start: data or reader qualified name to start with
    end: desired output type name; any match on any of the strings given
    cutoff: the maximum numer of steps to consider per path
    avoid: ignore all readers/converters with a name matching this

    Returns
    -------
    A list of paths, where each item is a list of steps (starttype, endtype) for which
    there is a conversion class.
    """
    import networkx as nx

    g = conversions_graph(avoid=avoid)
    cls_names = [(_, _.qname()) for _ in subclasses(BaseConverter)]
    alltypes = list(g)
    matchtypes = [_ for _ in alltypes if re.findall(start, _)]
    if not matchtypes:
        raise ValueError("type found no match: %s", start)
    start = matchtypes[0]
    if isinstance(end, str):
        end = (end,)
    matchtypes = [_ for _ in alltypes if any(re.findall(e, _) for e in end)]
    clss = [cls for cls, name in cls_names if any(re.findall(e, name) for e in end)]
    if len(matchtypes) + len(clss) == 0:
        raise ValueError("outtype found no match: %s", end)
    if clss:
        end = list(clss[0].instances.values())[0]
    else:
        end = matchtypes[0]
    return sorted(nx.all_simple_edge_paths(g, start, end, cutoff=cutoff), key=len)


def auto_pipeline(
    url: str | BaseData,
    outtype: str | tuple[str] = "",
    storage_options: dict | None = None,
    avoid: list[str] | None = None,
    prefer: list[str] | None = None,
    exclude: list[str] | None = None,
) -> Pipeline:
    """Create pipeline from given URL to desired output type

    Will search for the shortest conversion path from the inferred data-type to the
    output.

    Parameters
    ----------
    url: input data, usually a location/URL, but maybe a data instance
    outtype: pattern to match to possible output types (instance or last converter)
    storage_options: if url is a remote str, these are kwargs that fsspec may need to
        access it
    avoid: don't consider readers whose names match any of these strings
    prefer:
        List of substring patterns (case-insensitive) matched against reader class
        names.  Matching readers are tried *before* non-matching ones when multiple
        candidates satisfy the path.  Example: ``prefer=["Polars", "Duck"]``.
    exclude:
        List of substring patterns (case-insensitive) matched against reader class
        names.  Any reader whose class name matches is removed from consideration.
        Example: ``exclude=["Spark", "Ray"]``.
    """
    from intake.readers.datatypes import recommend

    if isinstance(url, str):
        if storage_options:
            data = recommend(url, storage_options=storage_options)[0](
                url=url, storage_options=storage_options
            )
        else:
            data = recommend(url)[0](url=url)
    else:
        data = url
    if isinstance(data, BaseData):
        start = data.qname()
        steps = path(start, outtype, avoid=avoid)
        if steps:
            for steps in steps:
                reader = data.to_reader(
                    outtype=steps[0][1] if steps else outtype,
                    prefer=prefer,
                    exclude=exclude,
                )
                try:
                    for n, s in enumerate(steps[1:]):
                        if n == len(steps) - 2:
                            if outtype == s[1]:
                                reader = reader.transform[s[1]]
                            else:
                                reader = reader.transform.get_by_attr(outtype, True)
                        else:
                            reader = reader.transform[s[1]]
                except ValueError:
                    # no reader - not importable
                    continue
                break
    elif isinstance(data, BaseReader):
        reader = data
        steps = path(data.output_instance, outtype, avoid=avoid)
        for s in steps[0]:
            reader = reader.transform.get_by_attr([s[1]], True)

    return reader
