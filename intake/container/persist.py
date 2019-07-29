#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import posixpath
import shutil
import time
import yaml
from ..catalog.local import YAMLFileCatalog, CatalogEntry
from .. import DataSource
from ..config import conf, logger
from ..source import import_name
from ..utils import make_path_posix


def _maybe_add_rm(fs):
    # monkey-path local filesystem
    # this goes away if we can use fsspec's local file-system
    from fsspec.implementations.local import LocalFileSystem
    if isinstance(fs, LocalFileSystem):
        def rm(path, recursive=False):
            if recursive:
                import shutil
                shutil.rmtree(path)
            else:
                import os
                os.remove(path)
        fs.rm = rm


class PersistStore(YAMLFileCatalog):
    """
    Specialised catalog for persisted data-sources
    """
    _singleton = [None]

    def __new__(cls, *args, **kwargs):
        # singleton pattern: only one instance will ever exist
        if cls._singleton[0] is None:
            o = object.__new__(cls)
            o._captured_init_args = args
            o._captured_init_kwargs = kwargs
            cls._singleton[0] = o
        return cls._singleton[0]

    def __init__(self, path=None):
        # from fsspec.registry import filesystem
        from fsspec import filesystem
        self.pdir = make_path_posix(path or conf.get('persist_path'))
        path = posixpath.join(self.pdir, 'cat.yaml')
        protocol = (self.pdir.split('://', 1)[0]
                    if "://" in self.pdir else 'file')
        self.fs = filesystem(protocol)
        _maybe_add_rm(self.fs)
        super(PersistStore, self).__init__(path)

    def _load(self):
        # try to make sure there's always something to load from
        try:
            self.fs.mkdirs(self.pdir)
        except (OSError, IOError):
            pass
        try:
            super(PersistStore, self)._load()
        except:
            # if destination doesn't load, we have no entries
            # likely will get exceptions if try to persist
            self._entries = {}

    def getdir(self, source):
        """Clear/create a directory to store a persisted dataset into"""
        subdir = posixpath.join(self.pdir, source._tok)
        try:
            self.fs.rm(subdir, True)
        except Exception as e:
            logger.debug("Directory clear failed: %s" % e)
        self.fs.mkdirs(subdir)
        return subdir

    def add(self, key, source):
        """Add the persisted source to the store under the given key

        key : str
            The unique token of the un-persisted, original source
        source : DataSource instance
            The thing to add to the persisted catalogue, referring to persisted
            data
        """
        from intake.catalog.local import LocalCatalogEntry
        try:
            with self.fs.open(self.path, 'rb') as f:
                data = yaml.safe_load(f)
        except IOError:
            data = {'sources': {}}
        ds = source._yaml()['sources'][source.name]
        data['sources'][key] = ds
        with self.fs.open(self.path, 'wb') as fo:
            fo.write(yaml.dump(data, default_flow_style=False).encode())
        self._entries[key] = LocalCatalogEntry(
            name=ds['metadata']['original_name'],
            direct_access=True,
            cache=[],
            parameters=[],
            catalog_dir=None,
            **data['sources'][key])

    def get_tok(self, source):
        """Get string token from object

        Strings are assumed to already be a token; if source or entry, see
        if it is a persisted thing ("original_tok" is in its metadata), else
        generate its own token.
        """
        if isinstance(source, str):
            return source

        if isinstance(source, CatalogEntry):
            return source._metadata.get('original_tok', source._tok)

        if isinstance(source, DataSource):
            return source.metadata.get('original_tok', source._tok)
        raise IndexError

    def remove(self, source, delfiles=True):
        """Remove a dataset from the persist store

        source : str or DataSource or Lo
            If a str, this is the unique ID of the original source, which is
            the key of the persisted dataset within the store. If a source,
            can be either the original or the persisted source.
        delfiles : bool
            Whether to remove the on-disc artifact
        """
        source = self.get_tok(source)
        with self.fs.open(self.path, 'rb') as f:
            data = yaml.safe_load(f.read().decode())
        data['sources'].pop(source, None)
        with self.fs.open(self.path, 'wb') as fo:
            fo.write(yaml.dump(data, default_flow_style=False).encode())
        if delfiles:
            path = posixpath.join(self.pdir, source)
            try:
                self.fs.rm(path, True)
            except Exception as e:
                logger.debug("Failed to delete persisted data dir %s" % path)
        self._entries.pop(source, None)

    def clear(self):
        """Remove all persisted sources, files and catalog"""
        self.fs.rm(self.pdir, True)

    def backtrack(self, source):
        """Given a unique key in the store, recreate original source"""
        key = self.get_tok(source)
        s = self[key]()
        meta = s.metadata['original_source']
        cls = meta['cls']
        args = meta['args']
        kwargs = meta['kwargs']
        cls = import_name(cls)
        sout = cls(*args, **kwargs)
        sout.metadata = s.metadata['original_metadata']
        sout.name = s.metadata['original_name']
        return sout

    def refresh(self, key):
        """Recreate and re-persist the source for the given unique ID"""
        s0 = self[key]
        s = self.backtrack(key)
        s.persist(**s0.metadata['persist_kwargs'])

    def needs_refresh(self, source):
        """Has the (persisted) source expired in the store

        Will return True if the source is not in the store at all, if it's
        TTL is set to None, or if more seconds have passed than the TTL.
        """
        now = time.time()
        if source._tok in self:
            s0 = self[source._tok]
            if self[source._tok].metadata.get('ttl', None):
                then = s0.metadata['timestamp']
                if s0.metadata['ttl'] < then - now:
                    return True
            return False
        return True


store = PersistStore()
