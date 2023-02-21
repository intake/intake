# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import gzip
import io
import pickle
from collections import OrderedDict

import msgpack

from ..compat import pack_kwargs


class NoneCompressor(object):
    name = "none"

    def compress(self, data):
        return data

    def decompress(self, data):
        return data


class GzipCompressor(object):
    name = "gzip"

    def compress(self, data):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=1) as f:
            f.write(data)
        return buf.getvalue()

    def decompress(self, data):
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            return f.read()


try:
    import msgpack_numpy
except ImportError:
    msgpack_numpy = None


def check_pyarrow():
    try:
        import pyarrow
    except ImportError:
        raise ImportError("Serializing DataFrames requires pyarrow.")
    return pyarrow


class MsgPackSerializer(object):
    # TODO: This is ugly, should maybe transition to
    #  distributed.protocol.serialize
    name = "msgpack"

    def encode(self, obj, container):
        if container in ["ndarray", "xarray"] and msgpack_numpy:
            from ..compat import np_pack_kwargs

            return msgpack.packb(obj, **np_pack_kwargs)
        elif container == "dataframe":
            # Use pyarrow for serializing DataFrames, rather than
            # msgpack: https://github.com/intake/intake/issues/460
            pa = check_pyarrow()

            context = pa.default_serialization_context()
            # This eventually goes to msgpack.packb, which doesn't
            # directly accept PyArrow Buffer objects. Need to wrap
            # it in a memoryview to avoid a TypeError.
            return memoryview(context.serialize(obj).to_buffer())
        else:
            return msgpack.packb(obj, **pack_kwargs)

    def decode(self, bytestr, container):
        from ..compat import unpack_kwargs

        if container in ["ndarray", "xarray"] and msgpack_numpy:
            from ..compat import np_unpack_kwargs

            return msgpack.unpackb(bytestr, **np_unpack_kwargs)
        elif container == "dataframe":
            pa = check_pyarrow()
            context = pa.default_serialization_context()
            return context.deserialize(bytestr)
        else:
            return msgpack.unpackb(bytestr, **unpack_kwargs)


class PickleSerializer(object):
    def __init__(self, protocol_level):
        self._protocol_level = protocol_level
        self.name = "pickle%d" % protocol_level

    def encode(self, obj, container):
        return pickle.dumps(obj, protocol=self._protocol_level)

    def decode(self, bytestr, container):
        return pickle.loads(bytestr)


class ComboSerializer(object):
    def __init__(self, format_encoder, compressor):
        self._format_encoder = format_encoder
        self._compressor = compressor
        self.format_name = format_encoder.name
        self.compressor_name = compressor.name

    def encode(self, obj, container):
        return self._compressor.compress(self._format_encoder.encode(obj, container))

    def decode(self, bytestr, container):
        return self._format_encoder.decode(self._compressor.decompress(bytestr), container)


compressors = [GzipCompressor(), NoneCompressor()]
try:
    import snappy

    class SnappyCompressor(object):
        name = "snappy"

        def compress(self, data):
            return snappy.compress(data)

        def decompress(self, data):
            return snappy.decompress(data)

    compressors.insert(0, SnappyCompressor())
except ImportError:
    pass


# Insert in preference order
picklers = [PickleSerializer(protocol) for protocol in [2, 1]]
serializers = [MsgPackSerializer()] + picklers
format_registry = OrderedDict([(e.name, e) for e in serializers])
compression_registry = OrderedDict([(e.name, e) for e in compressors])
