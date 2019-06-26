#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import msgpack
from .utils import encode_datetime, decode_datetime


if msgpack.version >= (0, 5, 2):
    unpack_kwargs = {'raw': False}
else:
    unpack_kwargs = {'encoding': 'utf-8'}

unpack_kwargs["object_hook"] = decode_datetime


pack_kwargs = dict(
    default=encode_datetime,
    use_bin_type=True,
)

try:
    import msgpack_numpy
    np_unpack_kwargs = dict(
        object_hook=lambda obj: decode_datetime(msgpack_numpy.decode(obj)),
    )
    np_pack_kwargs = dict(
        default=lambda obj: encode_datetime(msgpack_numpy.encode(obj)),
    )
except ImportError:
    pass


