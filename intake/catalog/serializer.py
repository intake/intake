from collections import OrderedDict


import msgpack
import msgpack_numpy
import pandas
import numpy
import pickle


class MsgPackSerializer:
    name = 'msgpack'

    def encode(self, obj, container):
        if container == 'dataframe':
            return obj.to_msgpack()
        elif container == 'ndarray':
            return msgpack.packb(obj, default=msgpack_numpy.encode)
        elif container == 'python':
            return msgpack.packb(obj, use_bin_type=True)
        else:
            raise ValueError('unknown container: %s' % container)

    def decode(self, bytestr, container):
        if container == 'dataframe':
            return pandas.read_msgpack(bytestr)
        elif container == 'ndarray':
            return msgpack.unpackb(bytestr, object_hook=msgpack_numpy.decode)
        elif container == 'python':
            return msgpack.unpackb(bytestr, encoding='utf-8')
        else:
            raise ValueError('unknown container: %s' % container)

class PickleSerializer:
    def __init__(self, protocol_level):
        self._protocol_level = protocol_level
        self.name = 'pickle%d' % protocol_level

    def encode(self, obj, container):
        return pickle.dumps(obj, protocol=self._protocol_level)

    def decode(self, bytestr, container):
        return pickle.loads(bytestr)


# Insert in preference order
registry = OrderedDict([(e.name, e) for e in 
                        [MsgPackSerializer(), PickleSerializer(4), PickleSerializer(2)]
                       ])

