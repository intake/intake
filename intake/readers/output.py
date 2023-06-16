"""Serialise and output data into persistent formats

This is how to "export" data from Intake.

By convention, functions here produce an instance of FileData, which can then be used
to produce new catalog entries.
"""

from intake.readers.convert import register_converter
from intake.readers.datatypes import Parquet


@register_converter("pandas:DataFrame", "intake.readers.datatypes:FileData")
@register_converter("dask.dataframe:DataFrame", "intake.readers.datatypes:FileData")
def pandas_to_parquet(x, url, storage_options=None, metadata=None, **kwargs):
    # metadata should be passed by pipeline to give details of the current run and perhaps
    # a repr, dtypes, shape of the data
    x.to_parquet(url, storage_options=storage_options, **kwargs)
    return Parquet(url=url, storage_options=storage_options, metadata=metadata)
