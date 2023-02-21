import contextlib
import json
from itertools import islice

from intake.source.base import DataSource


class JSONFileSource(DataSource):
    """
    Read JSON files as a single dictionary or list

    The files can be local or remote. Extra parameters for encoding, etc.,
    go into ``storage_options``.
    """

    name = "json"
    version = "0.0.1"
    container = "python"

    def __init__(
        self,
        urlpath: str,
        text_mode: bool = True,
        text_encoding: str = "utf8",
        compression: str = None,
        read: bool = True,
        metadata: dict = None,
        storage_options: dict = None,
    ):
        """
        Parameters
        ----------
        urlpath : str
            Target file. Can include protocol specified (e.g., "s3://").
        text_mode : bool
            Whether to open the file in text mode, recoding binary
            characters on the fly
        text_encoding : str
            If text_mode is True, apply this encoding. UTF* is by far the most
            common
        compression : str or None
            If given, decompress the file with the given codec on load. Can
            be something like "zip", "gzip", "bz2", or to try to guess from the
            filename, 'infer'
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        """
        from fsspec.utils import compressions

        VALID_COMPRESSIONS = list(compressions.values()) + ["infer"]

        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._file = None
        self.compression = compression
        if compression is not None:
            if compression not in VALID_COMPRESSIONS:
                raise ValueError(f"Compression value {compression} must be one of {VALID_COMPRESSIONS}")
        self.mode = "rt" if text_mode else "rb"
        self.encoding = text_encoding
        self._read = read

        super(JSONFileSource, self).__init__(metadata=metadata)

    def read(self):
        import fsspec

        urlpath = self._get_cache(self._urlpath)[0]
        with fsspec.open(
            urlpath,
            mode=self.mode,
            encoding=self.encoding,
            compression=self.compression,
            **self._storage_options,
        ) as f:
            return json.load(f)

    def _load_metadata(self):
        pass

    def _get_schema(self):
        pass


class JSONLinesFileSource(DataSource):
    """
    Read a JSONL (https://jsonlines.org/) file and return a list of objects,
    each being valid json object (e.g. a dictionary or list)
    """

    name = "jsonl"
    version = "0.0.1"
    container = "python"

    def __init__(
        self,
        urlpath: str,
        text_mode: bool = True,
        text_encoding: str = "utf8",
        compression: str = None,
        read: bool = True,
        metadata: dict = None,
        storage_options: dict = None,
    ):
        """
        Parameters
        ----------
        urlpath : str
            Target file. Can include protocol specified (e.g., "s3://").
        text_mode : bool
            Whether to open the file in text mode, recoding binary
            characters on the fly
        text_encoding : str
            If text_mode is True, apply this encoding. UTF* is by far the most
            common
        compression : str or None
            If given, decompress the file with the given codec on load. Can
            be something like "zip", "gzip", "bz2", or to try to guess from the
            filename, 'infer'.
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        """
        from fsspec.utils import compressions

        VALID_COMPRESSIONS = list(compressions.values()) + ["infer"]

        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._file = None
        self.compression = compression
        if compression is not None:
            if compression not in VALID_COMPRESSIONS:
                raise ValueError(f"Compression value {compression} must be one of {VALID_COMPRESSIONS}")
        self.mode = "rt" if text_mode else "rb"
        self.encoding = text_encoding
        self._read = read
        super().__init__(metadata=metadata)

    @contextlib.contextmanager
    def _open(self):
        """
        Yields an fsspec.OpenFile object
        """
        import fsspec

        urlpath = self._get_cache(self._urlpath)[0]
        with fsspec.open(
            urlpath,
            mode=self.mode,
            encoding=self.encoding,
            compression=self.compression,
            **self._storage_options,
        ) as f:
            yield f

    def read(self):
        with self._open() as f:
            return list(map(json.loads, f))

    def head(self, nrows: int = 100):
        """
        return the first `nrows` lines from the file
        """
        with self._open() as f:
            return list(map(json.loads, islice(f, nrows)))

    def _load_metadata(self):
        pass

    def _get_schema(self):
        pass
