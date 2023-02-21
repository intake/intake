# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from intake.container.base import RemoteSource, get_partition
from intake.source.base import Schema


class RemoteSequenceSource(RemoteSource):
    """Sequence-of-things source on an Intake server"""

    name = "remote_sequence"
    container = "python"

    def __init__(self, url, headers, **kwargs):
        self.url = url
        self.npartitions = kwargs.get("npartition", 1)
        self.partition_access = self.npartitions > 1
        self.headers = headers
        self.metadata = kwargs.get("metadata", {})
        self._schema = Schema(npartitions=self.npartitions, extra_metadata=self.metadata)
        self.bag = None
        super(RemoteSequenceSource, self).__init__(url, headers, **kwargs)

    def _load_metadata(self):
        import dask
        import dask.bag as db

        if self.bag is None:
            self.parts = [dask.delayed(get_partition)(self.url, self.headers, self._source_id, self.container, i) for i in range(self.npartitions)]
            self.bag = db.from_delayed(self.parts)
        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.parts[i].compute()

    def read(self):
        self._load_metadata()
        return self.bag.compute()

    def to_dask(self):
        self._load_metadata()
        return self.bag

    def _close(self):
        self.bag = None

    @staticmethod
    def _persist(source, path, encoder=None, **kwargs):
        """Save list to files using encoding

        encoder : None or one of str|json|pickle
            None is equivalent to str
        """
        import json
        import pickle

        encoder = {None: str, "str": str, "json": json.dumps, "pickle": pickle.dumps}[encoder]
        try:
            b = source.to_dask()
        except NotImplementedError:
            b = source.read()
        return RemoteSequenceSource._data_to_source(b, path, encoder, **kwargs)

    @staticmethod
    def _data_to_source(b, path, encoder=None, storage_options=None, **kwargs):
        import json
        import pickle
        import posixpath

        import dask
        import dask.bag as db
        from fsspec import open_files

        from intake.source.textfiles import TextFilesSource

        encoder = {None: str, "str": str, "json": json.dumps, "pickle": pickle.dumps}.get(encoder, encoder)

        if not hasattr(b, "to_textfiles"):
            try:
                b = db.from_sequence(b, npartitions=1)
            except TypeError:
                raise NotImplementedError

        files = open_files(posixpath.join(path, "part.*"), mode="wt", num=b.npartitions, **(storage_options or {}))
        dwrite = dask.delayed(write_file)
        out = [dwrite(part, f, encoder) for part, f in zip(b.to_delayed(), files)]
        dask.compute(out)
        s = TextFilesSource(posixpath.join(path, "part.*"), storage_options=storage_options)
        return s


def write_file(data, fo, encoder):
    with fo as f:
        for d in data:
            f.write(encoder(d))
