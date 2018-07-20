from .dataframe import RemoteDataFrame
from .ndarray import NdArray
from .semistructured import RemoteSequenceSource
from ..catalog.base import RemoteCatalog

container_map = {
    'dataframe': RemoteDataFrame,
    'python': RemoteSequenceSource,
    'ndarray': NdArray,
    'catalog': RemoteCatalog
}

__all__ = ['container_map']
