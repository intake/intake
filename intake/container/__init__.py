from .dataframe import RemoteDataFrame
from .ndarray import NdArray
from .semistructured import RemoteSequenceSource

container_map = {
    'dataframe': RemoteDataFrame,
    'python': RemoteSequenceSource,
    'ndarray': NdArray
}

__all__ = ['container_map']
