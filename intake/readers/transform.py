"""Manipulate data

By convention, functions that change the data but not the container type
"""
from intake.readers.convert import register_converter


@register_converter("pandas:DataFrame", "pandas:DataFrame")
@register_converter("dask.dataframe:DataFrame", "dask.dataframe:DataFrame")
def df_select_columns(x, columns, **_):
    return x[columns]


@register_converter("xarray:DataSet", "xarray:Dataset")
@register_converter("xarray:DataArray", "xarray:DataArray")
def xarray_sel(x, indexers, **_):
    return x.sel(indexers)


@register_converter("pyspark.sql:DataFrame", "pyspark.sql:DataFrame")
def pyspark_select_columns(x, columns, **_):
    return x.select(columns)


@register_converter(".*", ".*")  # SameType)
def method(x, method_name: str, *args, **kw):
    """Call named method on object

    Assumes output type is the same as input.
    """
    # TODO: there is no way to get args here right now
    return getattr(x, method_name)(*args, **kw)


@register_converter(".*", ".*")  # SameType)
def getitem(x, item, **kw):
    """Equivalent of x[item]

    Assumes output type is the same as input.
    """
    return x[item]
