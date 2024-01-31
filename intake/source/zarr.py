# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from .base import DataSource
from intake.readers.datatypes import Zarr
from intake.readers.readers import DaskZarr, NumpyZarr


class ZarrArraySource(DataSource):
    """Read Zarr format files into an array

    Zarr is an numerical array storage format which works particularly well
    with remote and parallel access.
    For specifics of the format, see https://zarr.readthedocs.io/en/stable/
    """

    container = "ndarray"
    name = "ndzarr"
    version = "0.0.1"
    partition_access = True

    def __init__(self, urlpath, storage_options=None, component=None, metadata=None):
        """

        Parameters
        ----------
        urlpath : str
            Location of data file(s), possibly including protocol
            information
        storage_options : dict
            Passed on to storage backend for remote files
        component : str or None
            If None, assume the URL points to an array. If given, assume
            the URL points to a group, and descend the group to find the
            array at this location in the hierarchy; components are separated
            by the "/" character.
        """
        self.data = Zarr(
            url=urlpath,
            storage_options=storage_options,
            root=component,
            metadata=metadata,
        )
        self.metadata = metadata

    def to_dask(self):
        return DaskZarr(self.data).read()

    def read(self):
        return NumpyZarr(self.data).read()
