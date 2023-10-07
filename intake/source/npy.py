# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from .base import DataSource
from intake.readers.datatypes import NumpyFile
from intake.readers.readers import DaskNPYStack, NumpyReader


class NPySource(DataSource):
    """Read numpy binary files into an array

    Prototype source showing example of working with arrays

    Each file becomes one or more partitions, but partitioning within a file
    is only along the largest dimension, to ensure contiguous data.
    """

    container = "ndarray"
    name = "numpy"
    version = "0.0.1"
    partition_access = True

    def __init__(self, path, storage_options=None, metadata=None):
        """
        The parameters dtype and shape will be determined from the first
        file, if not given.

        Parameters
        ----------
        path: str of list of str
            Location of data file(s), possibly including glob and protocol
            information
        storage_options: dict
            Passed to file-system backend.
        """
        self.data = NumpyFile(url=path, storage_options=storage_options, metadata=metadata)
        self.metadata = metadata

    def to_dask(self):
        return DaskNPYStack(self.data).read()

    def read(self):
        return NumpyReader(self.data).read()
