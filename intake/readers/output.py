"""Serialise and output data into persistent formats

This is how to "export" data from Intake.

By convention, functions here produce an instance of FileData (or other data type),
which can then be used to produce new catalog entries.
"""
import fsspec

from intake.readers.convert import register_converter
from intake.readers.datatypes import PNG, Feather2, Parquet, recommend


@register_converter("pandas:DataFrame", "intake.readers.datatypes:Parquet")
@register_converter("dask.dataframe:DataFrame", "intake.readers.datatypes:Parquet")
def pandas_to_parquet(x, url, storage_options=None, metadata=None, **kwargs):
    x.to_parquet(url, storage_options=storage_options, **kwargs)
    return Parquet(url=url, storage_options=storage_options, metadata=metadata)


@register_converter("pandas:DataFrame", "matplotlib.pyplot:Figure")
@register_converter("geopandas:GeoDataFrame", "matplotlib.pyplot:Figure")
def pandas_to_matplotlib_figure(x, **kwargs):
    # this could be a convert rather than an output
    import matplotlib.pyplot as plt

    fig = plt.Figure()
    ax = fig.add_subplot(111)
    x.plot(ax=ax, **kwargs)  # backend="matplotlib"?
    return fig


@register_converter("matplotlib.pyplot:Figure", "intake.readers.datatypes:PNG")
def matplotlib_figure_to_png(x, url, metadata=None, storage_options=None, **kwargs):
    """Take a matplotlib figure and save to PNG file

    This could be used to produce thumbnails if followed by FileByteReader;
    to use temporary storage rather than concrete files, can use memory: or caching filesystems
    """
    with fsspec.open(url, mode="wb", **(storage_options or {})) as f:
        x.savefig(f, format="png", **kwargs)
    return PNG(url=url, metadata=metadata, storage_options=storage_options)


@register_converter("geopandas:GeoDataFrame", "intake.readers.datatypes:GeoJSON")
def geopandas_to_file(x, url, metadata=None, **kwargs):
    """creates one of several output file types

    Uses url extension of explicit driver= kwarg
    """
    x.to_file(url, **kwargs)
    return recommend(url)[0](url=url, metadata=metadata)


@register_converter("geopandas:GeoDataFrame", "intake.readers.datatypes:Parquet")
def geopandas_to_parquet(x, url, metadata=None, **kwargs):
    x.to_parquet(url, **kwargs)
    return Parquet(url=url, metadata=metadata)


@register_converter("geopandas:GeoDataFrame", "intake.readers.datatypes:Feather2")
def geopandas_to_feather(x, url, metadata=None, **kwargs):
    x.to_feather(url, **kwargs)
    return Feather2(url=url, metadata=metadata)


@register_converter(".*", "builtins:str")
def repr(x, **_):
    # good for including "peek" at data in entries' metadata
    return repr(x)
