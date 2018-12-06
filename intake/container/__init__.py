#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from .dataframe import RemoteDataFrame
from .ndarray import RemoteArray
from .semistructured import RemoteSequenceSource
from ..catalog.base import RemoteCatalog

# each container type is represented in the remote by one of the classes in
# this dictionary
container_map = {
    'dataframe': RemoteDataFrame,
    'python': RemoteSequenceSource,
    'ndarray': RemoteArray,
    'catalog': RemoteCatalog
}

__all__ = ['container_map']
