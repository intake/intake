import pandas as pd
import xarray as xr
import dask
import pickle

from .base import BaseContainer

class DataArray(BaseContainer):

    @staticmethod
    def merge(arrs, dim=None):
        return xr.concat(arrs, dim=dim)

    @staticmethod
    def encode(arr):
        return pickle.dumps(arr, protocol=-1)

    @staticmethod
    def decode(pkl):
        return pickle.loads(pkl)

    @staticmethod
    def read(chunks):
        return DataArray.merge(chunks)