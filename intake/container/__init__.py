from .base import BaseContainer
from .dataframe import DataFrame
from .ndarray import NdArray
from .semistructured import SemiStructured
from .xarray import DataArray

container_map = {
    'dataframe' : DataFrame,
    'python' : SemiStructured,
    'ndarray' : NdArray,
    'dataarray' : DataArray
}

def get_container_klass(container):
    return container_map.get(container, BaseContainer)

__all__ = [
    'DataFrame', 'NdArray', 'SemiStructured', 'DataArray', 'get_container_klass']