"""Manipulate data: functions that change the data but not the container type
"""
from __future__ import annotations

from intake.readers.convert import BaseConverter, SameType
from intake.readers.utils import one_to_one


class DataFrameColumns(BaseConverter):
    instances = one_to_one({"pandas:DataFrame", "dask.dataframe:DataFrame"})
    func = "pandas:DataFrame.loc"

    def run(self, x, columns, **_):
        return x[columns]


class XarraySel(BaseConverter):
    instances = one_to_one({"xarray:Dataset", "xarray:DataArray"})
    func = "xarray:Dataset.sel"

    def run(self, x, indexers, **_):
        return x.sel(indexers)


class THREDDSCatToMergedDataset(BaseConverter):
    instances = {"intake.readers.catalogs:THREDDSCatalog": "xarray:Dataset"}

    def run(self, cat, path, driver="h5netcdf", xarray_kwargs=None, concat_kwargs=None, **_):
        """Merges multiple datasets into a single datasets.

        Recreates the merged-dataset functionality of intake-thredds

        This source takes a THREDDS URL and a path to descend down, and calls the
        combine function on all of the datasets found.

        Parameters
        ----------
        url : str
            Location of server
        path : str, list of str
            Subcats to follow; include glob characters (*, ?) in here for matching.
        driver : str
            Select driver to access data. Choose from 'netcdf' and 'opendap'.
        xarray_kwargs: dict
            kwargs to be passed to xr.open_dataset
        concat_kwargs: dict
            kwargs to be passed to xr.concat() filled by files opened by xr.open_dataset
            previously
        """
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


class PysparkColumns(BaseConverter):
    instances = {"pyspark.sql:DataFrame": "pyspark.sql:DataFrame"}

    def run(self, x, columns, **_):
        return x.select(columns)


class Method(BaseConverter):
    """Call named method on object

    Assumes output type is the same as input.
    """

    instances = {".*": SameType}

    def run(self, x, *args, method_name: str = "", **kw):
        method = getattr(x, method_name)
        if callable(method):
            return method(*args, **kw)
        else:
            return method


class GetItem(BaseConverter):
    """Equivalent of x[item]

    Assumes output type is the same as input.
    """

    instances = {".*": SameType}
    func = "operator:getitem"

    def _read(self, item, data=None):
        return data[item]
