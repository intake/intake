import dask

from .base import BaseContainer


class DataFrame(BaseContainer):

    @staticmethod
    def merge(parts):
        import pandas as pd
        return pd.concat(parts, ignore_index=True)

    @staticmethod
    def to_dask(parts, dtype):
        # Compat: prefer dtype to already be meta-like, but can construct
        # maybe should use dask.dataframe.utils.make_meta
        if hasattr(dtype, 'fields'):
            # compound np.dtype
            meta = {name: arg[0] for name, arg in dtype.fields.items()}
        else:
            # dataframe or dict
            meta = dtype
        return dask.dataframe.from_delayed(parts, meta=meta)

    @staticmethod
    def encode(obj):
        return obj.to_msgpack()

    @staticmethod
    def decode(bytestr):
        import pandas as pd
        return pd.read_msgpack(bytestr)

    @staticmethod
    def read(chunks):
        import pandas as pd
        return pd.concat(chunks)
