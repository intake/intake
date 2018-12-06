#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import msgpack


if msgpack.version >= (0, 5, 2):
    unpack_kwargs = {'raw': False}
else:
    unpack_kwargs = {'encoding': 'utf-8'}
