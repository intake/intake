"""Serialise and output data into persistent formats

This is how to "export" data from Intake.

By convention, functions here produce an instance of FileData (or other data type),
which can then be used to produce new catalog entries.
"""
import fsspec

from intake.readers.convert import BaseConverter
from intake.readers.datatypes import PNG, Feather2, Parquet, recommend
from intake.readers.utils import all_to_one

# TODO: superclass for output, so they show up differently in any graph viz?
#  *most* things here produce a datatypes:BaseData, but not all


class PandasToParquet(BaseConverter):
    instances = all_to_one({"pandas:DataFrame", "dask.dataframe:DataFrame", "geopandas:DataFrame"}, "intake.readers.datatypes:Parquet")

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_parquet(url, storage_options=storage_options, **kwargs)
        return Parquet(url=url, storage_options=storage_options, metadata=metadata)


class PandasToFeather(BaseConverter):
    instances = all_to_one({"pandas:DataFrame", "geopandas:DataFrame"}, "intake.readers.datatypes:Feather2")

    def run(self, x, url, storage_options=None, metadata=None, **kwargs):
        x.to_feather(url, storage_options=storage_options, **kwargs)
        return Feather2(url=url, storage_options=storage_options, metadata=metadata)


class ToMatplotlib(BaseConverter):
    instances = all_to_one({"pandas:DataFrame", "geopandas:GeoDataFrame", "xarray:DataSet"}, "matplotlib.pyplot:Figure")

    def run(self, x, **kwargs):
        import matplotlib.pyplot as plt

        fig = plt.Figure()
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
    func = "builtins.repr"
