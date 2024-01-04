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
    PNG,
    CatalogFile,
    Feather2,
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

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_parquet(url, storage_options=storage_options, **kwargs)
        return Parquet(url=url, storage_options=storage_options, metadata=metadata)


class PandasToCSV(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "dask.dataframe:DataFrame", "geopandas:GeoDataFrame"},
        "intake.readers.datatypes:CSV",
    )

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_csv(url, storage_options=storage_options, **kwargs)
        return CSV(url=url, storage_options=storage_options, metadata=metadata)


class PandasToHDF5(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "dask.dataframe:DataFrame"},
        "intake.readers.datatypes:HDF5",
    )

    def run(self, x, url, table, storage_options=None, metadata=None, **kwargs):
        x.to_hdf(url, table, storage_options=storage_options, **kwargs)
        return HDF5(url=url, path=table, storage_options=storage_options, metadata=metadata)


class PandasToFeather(BaseConverter):
    instances = all_to_one(
        {"pandas:DataFrame", "geopandas:GeoDataFrame"},
        "intake.readers.datatypes:Feather2",
    )

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        # TODO: fsspec output
        x.to_feather(url, storage_options=storage_options, **kwargs)
        return Feather2(url=url, storage_options=storage_options, metadata=metadata)


class XarrayToNetCDF(BaseConverter):
    instances = {"xarray:Dataset": "intake.readers.datatypes:HDF5"}

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
    func = "xarray:Dataset.to_zarr"

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

    def run(self, x, url, metadata=None, storage_options=None, **kwargs):
        with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
            x.savefig(f, format="png", **kwargs)
        return PNG(url=url, metadata=metadata, storage_options=storage_options)


class GeopandasToFile(BaseConverter):
    """creates one of several output file types

    Uses url extension or explicit driver= kwarg
    """

    instances = {"geopandas:GeoDataFrame": "intake.readers.datatypes:GeoJSON"}

    def run(self, x, url, metadata=None, **kwargs):
        x.to_file(url, **kwargs)
        return recommend(url)[0](url=url, metadata=metadata)


class Repr(BaseConverter):
    """good for including "peek" at data in entries' metadata"""

    instances = {".*": "builtins:str"}
    func = "builtins:repr"


class CatalogToJson(BaseConverter):
    instances = {"intake.readers.entry:Catalog": "intake.readers.datatypes:CatalogFile"}
    func = "intake.readers.entry:Catalog.to_yaml_file"

    def run(self, x, url, metadata=None, storage_options=None, **kwargs):
        if storage_options:
            kwargs.update(storage_options)
        x.to_yaml_file(url, **kwargs)
        return CatalogFile(url=url, storage_options=storage_options, metadata=metadata)
