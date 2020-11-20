#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from datetime import datetime
from hashlib import md5

import collections
import json
import logging
import os
import posixpath
import re
import shutil
import warnings

from intake.config import conf
from intake.utils import make_path_posix

logger = logging.getLogger('intake')


def sanitize_path(path):
    """Utility for cleaning up paths."""
    from fsspec.utils import infer_storage_options

    storage_option = infer_storage_options(path)

    protocol = storage_option['protocol']
    if protocol in ('http', 'https'):
        # Most FSs remove the protocol but not HTTPFS. We need to strip
        # it to match properly.
        path = os.path.normpath(path.replace("{}://".format(protocol), ''))
    elif protocol == 'file':
        # Remove trailing slashes from file paths.
        path = os.path.normpath(path)
        # Remove colons
        path = path.replace(':', '')
    # Otherwise we just make sure that path is posix
    return make_path_posix(path)


display = set()


class BaseCache(object):
    """
    Provides utilities for managing cached data files.

    Providers of caching functionality should derive from this, and appear
    as entries in ``registry``. The principle methods to override are
    ``_make_files()`` and ``_load()`` and ``_from_metadata()``.
    """
    # download block size in bytes
    blocksize = 5000000

    def __init__(self, driver, spec, catdir=None, cache_dir=None,
                 storage_options={}):
        """
        Parameters
        ----------
        driver: str
            Name of the plugin that can load catalog entry
        spec: dict
            Specification for caching the data source.
        cache_dir: str or None
            Explicit location of cache root directory
        catdir: str or None
            Directory containing the catalog from which this spec was made
        """
        self._driver = driver
        self._spec = spec
        cd = make_path_posix(cache_dir or conf['cache_dir'])
        if cd == 'catdir':
            if catdir is None:
                raise TypeError('cache_dir="catdir" only allowed when loaded'
                                'from a catalog file.')
            cd = posixpath.join(catdir, 'intake_cache')
        self._cache_dir = cd

        self._storage_options = storage_options
        self._metadata = CacheMetadata()

    def _ensure_cache_dir(self):
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)
        if os.path.isfile(self._cache_dir):
            raise Exception("Path for cache directory exists as a file: {}"
                            "".format(self._cache_dir))

    def _munge_path(self, cache_subdir, urlpath):
        import re

        path = sanitize_path(urlpath)

        if 'regex' in self._spec:
            regex = r'%s' % sanitize_path(self._spec['regex'])
            path = re.sub(regex, '', path)

        return posixpath.join(self._cache_dir, cache_subdir, path.lstrip('/\\'))

    def _hash(self, urlpath):
        return md5(
                str((os.path.basename(urlpath),
                     self._spec.get('regex', ''),
                     self._driver)).encode()
            ).hexdigest()

    def _path(self, urlpath, subdir=None):
        if subdir is None:
            subdir = self._hash(urlpath)
        cache_path = self._munge_path(subdir, urlpath)

        dirname = os.path.dirname(cache_path)
        if not os.path.exists(dirname):
            if not (dirname.startswith('https://') or
                    dirname.startswith('http://')):
                os.makedirs(dirname)

        return cache_path

    def _log_metadata(self, urlpath, original_path, cache_path):
        metadata = {
            'created': datetime.now().isoformat(),
            'original_path': original_path,
            'cache_path': cache_path
            }
        self._metadata.update(urlpath, metadata)

    def load(self, urlpath, output=None, **kwargs):
        """
        Downloads data from a given url, generates a hashed filename,
        logs metadata, and caches it locally.

        Parameters
        ----------

        urlpath: str, location of data
            May be a local path, or remote path if including a protocol specifier
            such as ``'s3://'``. May include glob wildcards.
        output: bool
            Whether to show progress bars; turn off for testing

        Returns
        -------
        List of local cache_paths to be opened instead of the remote file(s). If
        caching is disable, the urlpath is returned.
        """
        if conf.get('cache_disabled', False):
            return [urlpath]
        self.output = output if output is not None else conf.get(
            'cache_download_progress', True)

        cache_paths = self._from_metadata(urlpath)
        if cache_paths is None:
            files_in, files_out = self._make_files(urlpath)
            self._load(files_in, files_out, urlpath)
        cache_paths = self._from_metadata(urlpath)
        return cache_paths

    def _from_metadata(self, urlpath):
        """Return set of local URLs if files already exist"""
        md = self.get_metadata(urlpath)
        if md is not None:
            return [e['cache_path'] for e in md]

    def _load(self, files_in, files_out, urlpath, meta=True):
        """Download a set of files"""
        import dask
        out = []
        outnames = []
        for file_in, file_out in zip(files_in, files_out):
            cache_path = file_out.path
            outnames.append(cache_path)

            # If `_munge_path` did not find a match we want to avoid
            # writing to the urlpath.
            if cache_path == urlpath:
                continue

            if not os.path.isfile(cache_path):
                logger.debug("Caching file: {}".format(file_in.path))
                logger.debug("Original path: {}".format(urlpath))
                logger.debug("Cached at: {}".format(cache_path))
                if meta:
                    self._log_metadata(urlpath, file_in.path, cache_path)
                ddown = dask.delayed(_download)
                out.append(ddown(file_in, file_out, self.blocksize,
                                 self.output))
        dask.compute(*out)
        return outnames

    def _make_files(self, urlpath, **kwargs):
        """Make OpenFiles for all input/outputs"""
        raise NotImplementedError

    def get_metadata(self, urlpath):
        """
        Parameters
        ----------

        urlpath: str, location of data
            May be a local path, or remote path if including a protocol specifier
            such as ``'s3://'``. May include glob wildcards.

        Returns
        -------
        Metadata (dict) about a given urlpath.
        """
        return self._metadata.get(urlpath)

    def clear_cache(self, urlpath):
        """
        Clears cache and metadata for a given urlpath.

        Parameters
        ----------

        urlpath: str, location of data
            May be a local path, or remote path if including a protocol specifier
            such as ``'s3://'``. May include glob wildcards.
        """
        cache_entries = self._metadata.pop(urlpath, [])  # ignore if missing
        for cache_entry in cache_entries:
            try:
                os.remove(cache_entry['cache_path'])
            except (OSError, IOError):
                pass
        try:
            fn = os.path.dirname(cache_entry['cache_path'])
            os.rmdir(fn)
        except (OSError, IOError):
            logger.debug("Failed to remove cache directory: %s" % fn)

    def clear_all(self):
        """
        Clears all cache and metadata.
        """
        for urlpath in self._metadata.keys():
            self.clear_cache(urlpath)

        # Safely clean up anything else.
        if not os.path.isdir(self._cache_dir):
            return
        for subdir in os.listdir(self._cache_dir):
            try:
                fn = posixpath.join(self._cache_dir, subdir)
                if os.path.isdir(fn):
                    shutil.rmtree(fn)
                if os.path.isfile(fn):
                    os.remove(fn)
            except (OSError, IOError) as e:
                logger.warning(str(e))


def _download(file_in, file_out, blocksize, output=False):
    """Read from input and write to output file in blocks"""
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        if output:
            try:
                from tqdm.autonotebook import tqdm
            except ImportError:
                logger.warn("Cache progress bar requires tqdm to be installed:"
                            " conda/pip install tqdm")
                output = False
        try:
            file_size = file_in.fs.size(file_in.path)
            pbar_disabled = not file_size
        except ValueError as err:
            logger.debug("File system error requesting size: {}".format(err))
            file_size = 0
            pbar_disabled = True
        if output:
            if not pbar_disabled:
                for i in range(100):
                    if i not in display:
                        display.add(i)
                        out = i
                        break
                pbar = tqdm(total=file_size // 2 ** 20, leave=False,
                            disable=pbar_disabled,
                            position=out, desc=os.path.basename(file_out.path),
                            mininterval=0.1,
                            bar_format=r'{n}/|/{l_bar}')
            else:
                output = False

        logger.debug("Caching {}".format(file_in.path))
        with file_in as f1:
            with file_out as f2:
                data = True
                while data:
                    data = f1.read(blocksize if file_size else -1)
                    f2.write(data)
                    if output:
                        pbar.update(len(data) // 2**20)
        if output:
            try:
                pbar.update(pbar.total - pbar.n)  # force to full
                pbar.close()
            except Exception as e:
                logger.debug('tqdm exception: %s' % e)
            finally:
                display.remove(out)


class FileCache(BaseCache):
    """Cache specific set of files

    Input is a single file URL, URL with glob characters or list of URLs. Output
    is a specific set of local files.
    """

    def _make_files(self, urlpath, **kwargs):
        from fsspec import open_files

        self._ensure_cache_dir()
        if isinstance(urlpath, (list, tuple)):
            subdir = self._hash(":".join(urlpath))
        else:
            subdir = self._hash(urlpath)
        files_in = open_files(urlpath, 'rb', **self._storage_options)
        files_out = [open_files([self._path(f.path, subdir)], 'wb',
                                **self._storage_options)[0]
                     for f in files_in]
        return files_in, files_out

    def _from_metadata(self, urlpath):
        return super()._from_metadata(urlpath)


class DirCache(BaseCache):
    """Cache a complete directory tree

    Input is a directory root URL, plus a ``depth`` parameter for how many
    levels of subdirectories to search. All regular files will be copied. Output
    is the resultant local directory tree.
    """

    def _make_files(self, urlpath, **kwargs):
        from fsspec import open_files

        self._ensure_cache_dir()
        subdir = self._hash(urlpath)
        depth = self._spec['depth']
        files_in = []
        for i in range(1, depth + 1):
            files_in.extend(open_files('/'.join([urlpath] + ['*']*i)))
        files_out = [open_files([self._path(f.path, subdir)], 'wb',
                                **self._storage_options)[0]
                     for f in files_in]
        files_in2, files_out2 = [], []
        paths = set(os.path.dirname(f.path) for f in files_in)
        for fin, fout in zip(files_in, files_out):
            if fin.path in paths:
                try:
                    os.makedirs(fout.path)
                except Exception:
                    pass
            else:
                files_in2.append(fin)
                files_out2.append(fout)
        return files_in2, files_out2

    def _from_metadata(self, urlpath):
        """Return set of local URLs if files already exist"""
        md = self.get_metadata(urlpath)
        if md is not None:
            return [self._path(urlpath)]


class CompressedCache(BaseCache):
    """Cache files extracted from downloaded compressed source

    For one or more remote compressed files, downloads to local temporary dir and
    extracts all contained files to local cache. Input is URL(s) (including
    globs) pointing to remote compressed files, plus optional ``decomp``,
    which is "infer" by default (guess from file extension) or one of the
    key strings in ``intake.source.decompress.decomp``. Optional ``regex_filter``
    parameter is used to load only the extracted files that match the pattern.
    Output is the list of extracted files.
    """

    def _make_files(self, urlpath, **kwargs):
        import tempfile
        d = tempfile.mkdtemp()
        from fsspec import open_files

        self._ensure_cache_dir()
        self._urlpath = urlpath
        files_in = open_files(urlpath, 'rb', **self._storage_options)
        files_out = [open_files(
            [make_path_posix(os.path.join(d, os.path.basename(f.path)))],
            'wb')[0]
            for f in files_in
        ]
        super(CompressedCache, self)._load(files_in, files_out, urlpath,
                                           meta=False)
        return files_in, files_out

    def _load(self, files_in, files_out, urlpath, meta=True):
        from .decompress import decomp
        subdir = self._path(urlpath)
        try:
            os.makedirs(subdir)
        except (OSError, IOError):
            pass
        files = [f.path for f in files_out]
        out = []
        for f, orig in zip(files, files_in):
            # TODO: add snappy, brotli, lzo, lz4, xz... ?
            if 'decomp' in self._spec and self._spec['decomp'] != 'infer':
                d = self._spec['decomp']
            elif f.endswith('.zip'):
                d = 'zip'
            elif f.endswith(".tar.gz") or f.endswith('.tgz'):
                d = 'tgz'
            elif f.endswith(".tar.bz2") or f.endswith('.tbz'):
                d = 'tbz'
            elif f.endswith(".tar"):
                d = 'tar'
            elif f.endswith('.gz'):
                d = 'gz'
            elif f.endswith('.bz2'):
                d = 'bz'
            if d not in decomp:
                raise ValueError('Unknown compression for "%s"' % f)
            out2 = decomp[d](f, subdir)
            out3 = filter(re.compile(self._spec.get('regex_filter', '.*')).search, out2)
            for fn in out3:
                logger.debug("Caching file: {}".format(f))
                logger.debug("Original path: {}".format(orig.path))
                logger.debug("Cached at: {}".format(fn))
                if meta:
                    self._log_metadata(self._urlpath, orig.path, fn)
                out.append(fn)
        return out


class DATCache(BaseCache):
    r"""Use the DAT protocol to replicate data

    For details of the protocol, see https://docs.datproject.org/
    The executable ``dat`` must be available.

    Since in this case, it is not possible to access the remote files
    directly, this cache mechanism takes no parameters. The expectation
    is that the url passed by the driver is of the form:

    ::

        dat://<dat hash>/file_pattern

    where the file pattern will typically be a glob string like "\*.json".
    """

    def _make_files(self, urlpath, **kwargs):
        self._ensure_cache_dir()
        return None, None

    def _load(self, _, __, urlpath, meta=True):
        import subprocess
        from fsspec import open_files

        path = os.path.join(self._cache_dir, self._hash(urlpath))
        dat, part = os.path.split(urlpath)
        cmd = ['dat', 'clone', dat, path, '--no-watch']
        try:
            subprocess.call(cmd, stdout=subprocess.PIPE)
        except (IOError, OSError):  # pragma: no cover
            logger.info('Calling DAT failed')
            raise
        newpath = os.path.join(path, part)

        if meta:
            for of in open_files(newpath):
                self._log_metadata(urlpath, urlpath, of.path)


class CacheMetadata(collections.abc.MutableMapping):
    """
    Utility class for managing persistent metadata stored in the Intake config directory.
    """
    def __init__(self, *args, **kwargs):
        from intake import config

        self._path = posixpath.join(make_path_posix(config.confdir),
                                    'cache_metadata.json')
        d = os.path.dirname(self._path)
        if not os.path.exists(d):
            os.makedirs(d)

        if os.path.isfile(self._path):
            with open(self._path) as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {}

    def __getitem__(self, key):
        return self._metadata[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self._metadata[self.__keytransform__(key)] = value
        self._save()

    def __delitem__(self, key):
        del self._metadata[self.__keytransform__(key)]
        self._save()

    def __iter__(self):
        return iter(self._metadata)

    def __len__(self):
        return len(self._metadata)

    def __keytransform__(self, key):
        if isinstance(key, (list, tuple)):
            key = ":".join(key)
        return key

    def update(self, key, cache_entry):
        key = self.__keytransform__(key)
        entries = self._metadata.get(key, [])
        entries.append(cache_entry)
        self._metadata[key] = entries
        self._save()

    def _save(self):
        with open(self._path, 'w') as f:
            json.dump(self._metadata, f)

    def pop(self, key, default=None):
        item = self._metadata.pop(key, default)
        self._save()
        return item

    def keys(self):
        return list(self._metadata.keys())


registry = {
    'file': FileCache,
    'dir': DirCache,
    'compressed': CompressedCache,
    'dat': DATCache
}


def make_caches(driver, specs, catdir=None, cache_dir=None, storage_options={}):
    """
    Creates Cache objects from the cache_specs provided in the catalog yaml file

    Parameters
    ----------

    driver: str
        Name of the plugin that can load catalog entry
    specs: list
        Specification for caching the data source.
    """
    if specs is None:
        return []
    out = []
    for spec in specs:
        if 'type' in spec and spec['type'] not in registry:
            raise IndexError(spec['type'])
        out.append(registry.get(spec['type'], FileCache)(
            driver, spec, catdir=catdir, cache_dir=cache_dir,
            storage_options=storage_options))
    return out
