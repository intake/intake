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


@register_converter("intake.readers.catalogs:THREDDSCatalog", "xarray:Dataset")
def threadds_cat_to_merged_dataset(cat, path, driver="h5netcdf", xarray_kwargs=None, concat_kwargs=None, **_):
    import fnmatch

    import xarray as xr

    path = path.split("/") if isinstance(path, str) else path
    if driver not in ["pydap", "h5netcdf"]:
        raise ValueError
    xarray_kwargs = xarray_kwargs or {}
    xarray_kwargs["engine"] = driver
    for i, part in enumerate(path):
        if "*" not in part and "?" not in part:
            cat = cat[part]
        else:
            break
    path = "/".join(path)
    cat = cat.read()
    data = []
    suffix = {"pydap": "_DAP", "h5netcdf": "_CDF"}[driver]
    for name in list(cat):
        if fnmatch.fnmatch(name[:-4], path) and name[-4:] == suffix:
            data.append(cat[name].read(**xarray_kwargs))
    if concat_kwargs:
        return xr.concat(data, **concat_kwargs)
    else:
        return xr.combine_by_coords(data, combine_attrs="override")


@register_converter("pyspark.sql:DataFrame", "pyspark.sql:DataFrame")
def pyspark_select_columns(x, columns, **_):
    return x.select(columns)


@register_converter(".*", ".*")  # SameType)
def method(x, method_name: str, *args, **kw):
    """Call named method on object

    Assumes output type is the same as input.
    """
    return getattr(x, method_name)(*args, **kw)


@register_converter(".*", ".*")  # SameType)
def getitem(x, item, **kw):
    """Equivalent of x[item]

    Assumes output type is the same as input.
    """
    return x[item]
