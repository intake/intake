"""Serialise and output data into persistent formats

This is how to "export" data from Intake.

By convention, functions here produce an instance of FileData (or other data type),
which can then be used to produce new catalog entries.
"""
from __future__ import annotations

import fsspec

from intake.readers.convert import BaseConverter
from intake.readers.datatypes import (
    CSV,
    HDF5,
    JPEG,
    PNG,
    TIFF,
    WAV,
    ArrowIPC,
    CatalogFile,
    MetaImage,
    NRRD,
    NumpyFile,
    Parquet,
    Zarr,
    recommend,
)
from intake.readers.utils import all_to_one


# TODO: superclass for output, so they show up differently in any graph viz?
#  *most* things here produce a datatypes:BaseData, but not all


class PandasToParquet(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "dask.dataframe:DataFrame", "geopandas:GeoDataFrame"},
        "intake.readers.datatypes:Parquet",
    )
    func = "pandas:DataFrame.to_parquet"

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_parquet(url, storage_options=storage_options, **kwargs)
        return Parquet(url=url, storage_options=storage_options, metadata=metadata)


class PandasToCSV(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "dask.dataframe:DataFrame", "geopandas:GeoDataFrame"},
        "intake.readers.datatypes:CSV",
    )
    func = "pandas:DataFrame.to_csv"

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_csv(url, storage_options=storage_options, **kwargs)
        return CSV(url=url, storage_options=storage_options, metadata=metadata)


class PandasToHDF5(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "dask.dataframe:DataFrame"},
        "intake.readers.datatypes:HDF5",
    )
    func = "pandas:DataFrame.to_hdf"

    def run(self, x, url, table, storage_options=None, metadata=None, **kwargs):
        x.to_hdf(url, table, storage_options=storage_options, **kwargs)
        return HDF5(url=url, path=table, storage_options=storage_options, metadata=metadata)


class PandasToFeather(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "geopandas:GeoDataFrame"},
        "intake.readers.datatypes:ArrowIPC",
    )
    func = "pandas:DataFrame.to_feather"

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        # TODO: fsspec output
        x.to_feather(url, storage_options=storage_options, **kwargs)
        return ArrowIPC(url=url, storage_options=storage_options, metadata=metadata)


class XarrayToNetCDF(BaseConverter):
    instances = {"xarray:Dataset": "intake.readers.datatypes:HDF5"}
    func = "xarray:Dataset.to_netcdf"

    def run(self, x, url, group="", metadata=None, **kwargs):
        x.to_netcdf(path=url, group=group or None, **kwargs)
        return HDF5(url, path=group, metadata=metadata)


class XarrayToZarr(BaseConverter):
    instances = {"xarray:Dataset": "intake.readers.datatypes:Zarr"}
    func = "xarray:Dataset.to_zarr"

    def run(self, x, url, group="", storage_options=None, metadata=None, **kwargs):
        x.to_zarr(store=url, group=group or None, storage_options=storage_options, **kwargs)
        return Zarr(url, storage_options=storage_options, root=group, metadata=metadata)


class DaskArrayToZarr(BaseConverter):
    instances = {"dask.array:Array": "intake.readers.datatypes:Zarr"}
    func = "dask.array:Array.to_zarr"

    def run(self, x, url, group="", storage_options=None, metadata=None, **kwargs):
        x.to_zarr(
            store=url,
            component=group or None,
            storage_options=storage_options,
            **kwargs,
        )
        return Zarr(url, storage_options=storage_options, root=group, metadata=metadata)


class NumpyToNumpyFile(BaseConverter):
    """Save a single array into a single binary file"""

    instances = {"numpy:ndarray": "intake.readers.datatypes:NumpyFile"}
    func = "numpy:save"

    def run(self, x, path, *args, storage_options=None, metadata=None, **kwargs):
        if storage_options or "://" in path or "::" in path:
            with fsspec.open(path, **storage_options) as f:
                self._func(x, f)
        else:
            self._func(x, path)
        return NumpyFile(path, storage_options, metadata=metadata)


class ToMatplotlib(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "geopandas:GeoDataFrame", "xarray:Dataset"},
        "matplotlib.pyplot:Figure",
    )
    func = "matplotlib.pyplot:Figure"
    func_doc = "matplotlib.pyplot:plot"

    def run(self, x, **kwargs):
        fig = self._func()
        ax = fig.add_subplot(111)
        x.plot(ax=ax, **kwargs)
        return fig


class MatplotlibToPNG(BaseConverter):
    """Take a matplotlib figure and save to PNG file

    This could be used to produce thumbnails if followed by FileByteReader;
    to use temporary storage rather than concrete files, can use memory: or caching filesystems
    """

    instances = {"matplotlib.pyplot:Figure": "intake.readers.datatypes:PNG"}
    func = "matplotlib.pyplot:Figure.savefig"

    def run(self, x, url, metadata=None, storage_options=None, **kwargs):
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            x.savefig(f, format="png", **kwargs)
        return PNG(url=url, metadata=metadata, storage_options=storage_options)


class GeopandasToFile(BaseConverter):
    """creates one of several output file types

    Uses url extension or explicit driver= kwarg
    """

    instances = {"geopandas:GeoDataFrame": "intake.readers.datatypes:GeoJSON"}
    func = "geopandas:GeoDataFrame.to_file"

    def run(self, x, url, metadata=None, **kwargs):
        x.to_file(url, **kwargs)
        return recommend(url)[0](url=url, metadata=metadata)


class Repr(BaseConverter):
    """good for including "peek" at data in entries' metadata"""

    instances = {".*": "builtins:str"}
    func = "builtins:repr"


class IPythonDisplay(BaseConverter):
    func = "builtins:repr"  # fallback; actual display uses _repr_*_ protocol
    func_doc = "builtins:repr"
    instances = all_to_one(
        (
            "pandas:DataFrame",
            "pyspark.sql:DataFrame",
            "xarray:Dataset",
            "xarray:DataArray",
            "xarray:DataTree",
            "polars:LazyFrame",
            "dask.array:Array",
            "dask.dataframe:DataFrame",
            "duckdb:DuckDBPyRelation",
            "panel.pane:Image",
            "awkward:Array",
            "numpy:ndarray",
            "PIL.Image:Image",
        ),
        "builtins:dict",
    )

    def run(self, x, **kwargs):
        """Produce ipython/jupyter compatible output without imports"""
        import io

        # Special-case: PIL Image → inline PNG bytes
        try:
            from PIL import Image as _PILImage

            if isinstance(x, _PILImage.Image):
                buf = io.BytesIO()
                x.save(buf, format="PNG")
                return {"image/png": buf.getvalue()}
        except ImportError:
            pass

        # Special-case: 2-D or 3-D uint8 numpy array → inline PNG bytes
        try:
            import numpy as np
            from PIL import Image as _PILImage

            if isinstance(x, np.ndarray) and x.ndim in (2, 3):
                img = _PILImage.fromarray(x if x.dtype == np.uint8 else x.astype(np.uint8))
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                return {"image/png": buf.getvalue()}
        except ImportError:
            pass

        types = {
            "html": "text/html",
            "svg": "image/svg+xml",
            "png": "image/png",
            "jpeg": "image/jpeg",
            "pretty": "text/plain",
        }
        for name, mime in types.items():
            if hasattr(x, f"_repr_{name}_"):
                out = getattr(x, f"_repr_{name}_")()
                # out can be (data, metadata) for some calls
                # https://ipython.readthedocs.io/en/stable/config/integrating.html#metadata
                return {mime: out[0] if isinstance(out, tuple) else out}
        if hasattr(x, "_repr_mimebundle_"):
            return x._repr_mimebundle_()
        return {"text/plain": repr(x)}


class CatalogToJson(BaseConverter):
    instances = {"intake.readers.entry:Catalog": "intake.readers.datatypes:CatalogFile"}
    func = "intake.readers.entry:Catalog.to_yaml_file"

    def run(self, x, url, metadata=None, storage_options=None, **kwargs):
        if storage_options:
            kwargs.update(storage_options)
        x.to_yaml_file(url, **kwargs)
        return CatalogFile(url=url, storage_options=storage_options, metadata=metadata)


# ---------------------------------------------------------------------------
# Image output
# ---------------------------------------------------------------------------


class NumpyToPNG(BaseConverter):
    """Save a NumPy image array to a PNG file via Pillow.

    The array should be uint8 with shape ``(H, W)`` (grayscale), ``(H, W, 3)``
    (RGB), or ``(H, W, 4)`` (RGBA).  Float arrays in ``[0, 1]`` are
    automatically scaled to uint8.
    Returns a :class:`~intake.readers.datatypes.PNG` datatype descriptor so the
    result can be catalogued or chained into further reads.
    """

    instances = {"numpy:ndarray": "intake.readers.datatypes:PNG"}
    func = "PIL.Image:Image.save"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a 2-D or 3-D numeric array suitable for image encoding."""
        return getattr(x, "ndim", 0) in (2, 3) and getattr(x, "dtype", None) is not None

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        from PIL import Image
        import numpy as np

        if x.dtype.kind == "f":
            x = (np.clip(x, 0.0, 1.0) * 255).astype(np.uint8)
        img = Image.fromarray(x)
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            img.save(f, format="PNG", **kwargs)
        return PNG(url=url, storage_options=storage_options, metadata=metadata)


class NumpyToTIFF(BaseConverter):
    """Save a NumPy array to a TIFF file via ``tifffile``.

    Supports multi-dimensional arrays (e.g. z-stacks, time-series, multi-channel
    images) and preserves the full numeric dtype.  Pass ``resolution``,
    ``resolutionunit``, or any other ``tifffile.imwrite`` keyword argument via
    ``**kwargs``.
    Returns a :class:`~intake.readers.datatypes.TIFF` datatype descriptor.
    """

    instances = {"numpy:ndarray": "intake.readers.datatypes:TIFF"}
    func = "tifffile:imwrite"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a numeric array (integer or float dtype)."""
        return getattr(getattr(x, "dtype", None), "kind", None) in ("u", "i", "f", "c")

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        import tifffile

        if storage_options or "://" in url or "::" in url:
            with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
                tifffile.imwrite(f, x, **kwargs)
        else:
            tifffile.imwrite(url, x, **kwargs)
        return TIFF(url=url, storage_options=storage_options, metadata=metadata)


class PILImageToPNG(BaseConverter):
    """Save a PIL Image to a PNG file.

    Returns a :class:`~intake.readers.datatypes.PNG` datatype descriptor.
    """

    instances = {"PIL.Image:Image": "intake.readers.datatypes:PNG"}
    func = "PIL.Image:Image.save"

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            x.save(f, format="PNG", **kwargs)
        return PNG(url=url, storage_options=storage_options, metadata=metadata)


class PILImageToJPEG(BaseConverter):
    """Save a PIL Image to a JPEG file.

    Returns a :class:`~intake.readers.datatypes.JPEG` datatype descriptor.
    Pass ``quality`` (1–95) as a keyword argument to control compression.
    """

    instances = {"PIL.Image:Image": "intake.readers.datatypes:JPEG"}
    func = "PIL.Image:Image.save"

    def run(self, x, url, storage_options=None, metadata=None, quality=85, **kwargs):
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            x.save(f, format="JPEG", quality=quality, **kwargs)
        return JPEG(url=url, storage_options=storage_options, metadata=metadata)


class PILImageToTIFF(BaseConverter):
    """Save a PIL Image to a TIFF file.

    Returns a :class:`~intake.readers.datatypes.TIFF` datatype descriptor.
    """

    instances = {"PIL.Image:Image": "intake.readers.datatypes:TIFF"}
    func = "PIL.Image:Image.save"

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            x.save(f, format="TIFF", **kwargs)
        return TIFF(url=url, storage_options=storage_options, metadata=metadata)


# ---------------------------------------------------------------------------
# Audio output
# ---------------------------------------------------------------------------


class NumpyToWAV(BaseConverter):
    """Write a NumPy audio array to a WAV file via ``soundfile``.

    ``samplerate`` is required (e.g. ``44100``).  The array should have shape
    ``(n_samples,)`` for mono or ``(n_samples, n_channels)`` for multi-channel
    audio.  Any dtype supported by ``soundfile`` (int16, int32, float32,
    float64) is accepted.
    Returns a :class:`~intake.readers.datatypes.WAV` datatype descriptor.
    """

    instances = {"numpy:ndarray": "intake.readers.datatypes:WAV"}
    func = "soundfile:write"

    @classmethod
    def is_ok(cls, x) -> bool:
        """Require a 1-D or 2-D numeric array (mono or multi-channel audio)."""
        return getattr(x, "ndim", 0) in (1, 2) and getattr(
            getattr(x, "dtype", None), "kind", None
        ) in ("i", "u", "f")

    def run(self, x, url, samplerate, storage_options=None, metadata=None, **kwargs):
        import soundfile as sf

        if storage_options or "://" in url or "::" in url:
            with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
                sf.write(f, x, samplerate, **kwargs)
        else:
            sf.write(url, x, samplerate, **kwargs)
        return WAV(url=url, storage_options=storage_options, metadata=metadata)


# ---------------------------------------------------------------------------
# Scientific image output
# ---------------------------------------------------------------------------


class SimpleITKToNRRD(BaseConverter):
    """Write a SimpleITK Image to an NRRD file via ``SimpleITK.WriteImage``.

    Preserves spacing, origin, and direction metadata stored in the ITK object.
    Returns a :class:`~intake.readers.datatypes.NRRD` datatype descriptor.
    """

    instances = {"SimpleITK:Image": "intake.readers.datatypes:NRRD"}
    func = "SimpleITK:WriteImage"

    def run(self, x, url, metadata=None, **kwargs):
        import SimpleITK as sitk

        # SimpleITK.WriteImage works on local paths only
        sitk.WriteImage(x, url, **kwargs)
        return NRRD(url=url, metadata=metadata)


class SimpleITKToMetaImage(BaseConverter):
    """Write a SimpleITK Image to a MetaImage (.mha / .mhd) file.

    Returns a :class:`~intake.readers.datatypes.MetaImage` datatype descriptor.
    """

    instances = {"SimpleITK:Image": "intake.readers.datatypes:MetaImage"}
    func = "SimpleITK:WriteImage"

    def run(self, x, url, metadata=None, **kwargs):
        import SimpleITK as sitk

        sitk.WriteImage(x, url, **kwargs)
        return MetaImage(url=url, metadata=metadata)
