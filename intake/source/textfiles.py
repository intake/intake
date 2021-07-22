#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from . import base, import_name


class TextFilesSource(base.DataSource):
    """Read textfiles as sequence of lines

    Prototype of sources reading sequential data.

    Takes a set of files, and returns an iterator over the text in each of them.
    The files can be local or remote. Extra parameters for encoding, etc.,
    go into ``storage_options``.
    """
    name = 'textfiles'
    version = '0.0.1'
    container = 'python'
    partition_access = True

    def __init__(self, urlpath, text_mode=True, text_encoding='utf8',
                 compression=None, decoder=None, read=True, metadata=None,
                 storage_options=None):
        """

        Parameters
        ----------
        urlpath : str or list(str)
            Target files. Can be a glob-path (with "*") and include protocol
            specified (e.g., "s3://"). Can also be a list of absolute paths.
        text_mode : bool
            Whether to open the file in text mode, recoding binary
            characters on the fly
        text_encoding : str
            If text_mode is True, apply this encoding. UTF* is by far the most
            common
        compression : str or None
            If given, decompress the file with the given codec on load. Can
            be something like "gzip", "bz2", or to try to guess from the filename,
            'infer'
        decoder : function, str or None
            Use this to decode the contents of files. If None, you will get
            a list of lines of text/bytes. If a function, it must operate on
            an open file-like object or a bytes/str instance, and return a
            list
        read : bool
            If decoder is not None, this flag controls whether bytes/str get
            passed to the function indicated (True) or the open file-like
            object (False)
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        """
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._files = None
        if isinstance(decoder, str):
            decoder = import_name(decoder)
        self.decoder = decoder
        self.compression = compression
        self.mode = 'rt' if text_mode else 'rb'
        self.encoding = text_encoding
        self._read = read

        super(TextFilesSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from fsspec import open_files
        if self._files is None:

            urlpath = self._get_cache(self._urlpath)[0]

            self._files = open_files(
                urlpath, mode=self.mode, encoding=self.encoding,
                compression=self.compression,
                **self._storage_options)
            self.npartitions = len(self._files)
        return base.Schema(dtype=None,
                           shape=(None, ),
                           npartitions=self.npartitions,
                           extra_metadata=self.metadata)

    def _get_partition(self, i):
        return get_file(self._files[i], self.decoder, self._read)

    def read(self):
        self._get_schema()
        return self.to_dask().compute()

    def to_spark(self):
        from intake_spark.base import SparkHolder
        h = SparkHolder(False, [
            ('textFile', (self._urlpath, ))
        ], {})
        return h.setup()

    def to_dask(self):
        import dask.bag as db
        from dask import delayed
        self._get_schema()
        dfile = delayed(get_file)
        return db.from_delayed([dfile(f, self.decoder, self._read)
                                for f in self._files])


def get_file(f, decoder, read):
    """Serializable function to take an OpenFile object and read lines"""
    with f as f:
        if decoder is None:
            return list(f)
        else:
            d = f.read() if read else f
            out = decoder(d)
            if isinstance(out, (tuple, list)):
                return out
            else:
                return [out]
