class BaseContainer(object):

    @staticmethod
    def merge(parts):
        raise NotImplementedError

    @staticmethod
    def to_dask(parts, dtype):
        raise NotImplementedError

    @staticmethod
    def encode(obj):
        raise NotImplementedError

    @staticmethod
    def decode(bytestr):
        raise NotImplementedError

    @staticmethod
    def read(chunks):
        raise NotImplementedError