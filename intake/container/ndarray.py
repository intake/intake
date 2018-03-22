import msgpack
import numpy as np
import msgpack_numpy

from .base import BaseContainer

class NdArray(BaseContainer):

    @staticmethod
    def merge(parts):
        raise Exception('Need to implement ndarray case')

    @staticmethod
    def to_dask(parts, dtype):
        raise Exception('Need to implement ndarray case')

    @staticmethod
    def encode(obj):
        return msgpack.packb(obj, default=msgpack_numpy.encode)


    @staticmethod
    def decode(bytestr):
        return msgpack.unpackb(bytestr, object_hook=msgpack_numpy.decode)

    @staticmethod
    def read(chunks):
        return np.concatenate(chunks, axis=0)