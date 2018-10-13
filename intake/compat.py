
import msgpack


if msgpack.version >= (0, 5, 2):
    unpack_kwargs = {'raw': False}
else:
    unpack_kwargs = {'encoding': 'utf-8'}
