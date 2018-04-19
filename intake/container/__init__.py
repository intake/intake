from .base import BaseContainer
from .dataframe import DataFrame
from .ndarray import NdArray
from .semistructured import SemiStructured

container_map = {
    'dataframe': DataFrame,
    'python': SemiStructured,
    'ndarray': NdArray
}
__all__ = ['DataFrame', 'NdArray', 'SemiStructured', 'get_container_klass']


def get_container_klass(container):
    return container_map.get(container, BaseContainer)
