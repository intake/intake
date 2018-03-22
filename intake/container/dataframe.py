import pandas as pd
import dask

from .base import BaseContainer

class DataFrame(BaseContainer):

    @staticmethod
    def merge(parts, dim=None):
        return pd.concat(parts, ignore_index=True)

    @staticmethod
    def to_dask(parts, dtype=None):
        # Construct metadata
        meta = {name: arg[0] for name, arg in dtype.fields.items()}
        return dask.dataframe.from_delayed(parts, meta=meta)

    @staticmethod
    def encode(obj):
        return obj.to_msgpack()

    @staticmethod
    def decode(bytestr):
        return pd.read_msgpack(bytestr)

    @staticmethod
    def read(chunks):
        return pd.concat(chunks)