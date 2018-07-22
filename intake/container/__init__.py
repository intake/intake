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
