"""Manipulate data

By convention, functions change the data but not the container type
"""

from intake.readers.convert import register_converter


@register_converter("pandas:DataFrame", "pandas:DataFrame")
@register_converter("dask.dataframe:DataFrame", "dask.dataframe:DataFrame")
def select_columns(x, columns, **_):
    return x[columns]
