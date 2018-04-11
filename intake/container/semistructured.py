import dask
import msgpack
import operator

from .base import BaseContainer

class SemiStructured(BaseContainer):

    @staticmethod
    def merge(parts):
        # This seems to be the fastest way to do this for large lists
        data = []
        for p in parts:
            data.extend(p)
        return data

    @staticmethod
    def to_dask(parts, dtype):
        return dask.bag.from_delayed(parts)

    @staticmethod
    def encode(obj):
        return msgpack.packb(obj, use_bin_type=True)
        
    @staticmethod
    def decode(bytestr):
        return msgpack.unpackb(bytestr, encoding='utf-8')

    @staticmethod
    def read(chunks):
        return reduce(operator.add, chunks)