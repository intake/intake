# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from . import base
from intake.readers.datatypes import Text
from intake.readers.readers import SparkText, FileByteReader


class TextFilesSource(base.DataSource):
    """Read textfiles as sequence of lines

    Prototype of sources reading sequential data.

    Takes a set of files, and returns an iterator over the text in each of them.
    The files can be local or remote. Extra parameters for encoding, etc.,
    go into ``storage_options``.
    """

    name = "textfiles"
    version = "0.0.1"
    container = "python"
    partition_access = True

    def __init__(
        self,
        urlpath,
        text_mode=True,
        text_encoding="utf8",
        compression=None,
        decoder=None,
        metadata=None,
        storage_options=None,
    ):
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
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        """
        if compression:
            storage_options["compression"] = compression
        self.data = Text(url=urlpath, storage_options=storage_options, metadata=metadata)
        self.metadata = metadata
        self.kwargs = dict(text_mode=text_mode, text_encoding=text_encoding, decoder=decoder)

    def read(self):
        reader = FileByteReader(self.data)
        if self.kwargs["text_mode"]:
            if self.kwargs["decoder"]:
                reader = reader.apply(self.kwargs["decoder"])
            else:
                reader = reader.apply(bytes.decode, encoding=self.kwargs["text_encoding"])
        return reader.read()

    def to_spark(self):
        return SparkText(self.data).read()
