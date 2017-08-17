import glob

import pandas as pd

from . import base


class Plugin(base.Plugin):
    def __init__(self):
        pass

    def open(self, file_pattern, **kwargs):
        return CSVSource(file_pattern=file_pattern, pandas_kwargs=kwargs)


class CSVSource(base.DataSource):
    def __init__(self, file_pattern, pandas_kwargs):
        self._filenames = sorted(glob.glob(file_pattern))
        self._pandas_kwargs = pandas_kwargs

        # Detect shape
        datashape=None
        dtype=None
        shape=None
        super().__init__(datashape=datashape, 
                         dtype=dtype,
                         shape=shape,
                         container='dataframe')

    def read(self):
        parts = []

        for filename in self._filenames:
            parts.append(pd.read_csv(filename, **self._pandas_kwargs))
        
        return pd.concat(parts)

    def read_chunks(self, chunksize):
        for filename in self._filenames:
            reader = pd.read_csv(filename, chunksize=chunksize, **self._pandas_kwargs)
            for chunk in reader:
                yield chunk

    def close(self):
        pass # nothing to close
