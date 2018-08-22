from datetime import datetime
from hashlib import md5
from pathlib import Path

import collections
import json
import logging
import os
import shutil

from dask.bytes.utils import infer_storage_options
from intake.config import conf

logger = logging.getLogger('intake')


def sanitize_path(path):
    "Utility for cleaning up paths."

    storage_option = infer_storage_options(path)

    protocol = storage_option['protocol']
    if protocol in ('http', 'https'):
        # Most FSs remove the protocol but not HTTPFS. We need to strip
        # it to match properly.
        return os.path.normpath(path.replace("{}://".format(protocol), ''))
    elif protocol == 'file':
        # Just removing trailing slashes from file paths.
        return os.path.normpath(path)
    # Otherwise we leave the path alone
    return path


display = set()


class BaseCache(object):
    """
    Provides utilities for managing cached data files.
    """
    # download block size in bytes
    blocksize = 5000000

    def __init__(self, driver, spec, cache_dir=None):
        """
        Parameters:
        -----------
        driver: str
            Name of the plugin that can load catalog entry
        spec: list
            Specification for caching the data source.
        """
        self._driver = driver
        self._spec = spec
        self._cache_dir = cache_dir or os.getenv('INTAKE_CACHE_DIR',
                                                 conf['cache_dir'])
                             
        self._metadata = CacheMetadata()
    
    def _ensure_cache_dir(self):
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)
        if os.path.isfile(self._cache_dir):
            raise Exception("Path for cache directory exists as a file: {}".format(self._cache_dir))

    def _munge_path(self, cache_subdir, urlpath):
        import re

        regex = sanitize_path(self._spec['regex'])
        path = sanitize_path(urlpath)

        cache_path = re.sub(
            r"%s" % regex,
            os.path.join(self._cache_dir, cache_subdir),
            path
        )

        return urlpath if path == cache_path else cache_path

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

        Parameters:
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
        output = output if output is not None else conf.get(
            'cache_download_progress', True)

        cache_paths = []
        files_in, files_out = self._make_files(urlpath)
        self._load(files_in, files_out, cache_paths, urlpath, output)

        return cache_paths

    def _load(self, files_in, files_out, cache_paths, urlpath, output):
        """Download a set of files"""
        import dask
        out = []
        for file_in, file_out in zip(files_in, files_out):
            cache_path = file_out.path
            cache_paths.append(cache_path)

            # If `_munge_path` did not find a match we want to avoid
            # writing to the urlpath.
            if cache_path == urlpath:
                continue

            if not os.path.isfile(cache_path):
                logger.debug("Caching file: {}".format(file_in.path))
                logger.debug("Original path: {}".format(urlpath))
                logger.debug("Cached at: {}".format(cache_path))
                self._log_metadata(urlpath, file_in.path, cache_path)
                ddown = dask.delayed(_download)
                out.append(ddown(file_in, file_out, self.blocksize, output))
        dask.compute(*out)

    def _make_files(self, urlpath):
        """Make OpenFiles for all input/outputs"""
        raise NotImplementedError

    def get_metadata(self, urlpath):
        """
        Parameters:
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

        Parameters:
        ----------
        urlpath: str, location of data
            May be a local path, or remote path if including a protocol specifier
            such as ``'s3://'``. May include glob wildcards.
        """
        cache_entries = self._metadata.pop(urlpath)
        for cache_entry in cache_entries:
            try:
                os.remove(cache_entry['cache_path'])
            except FileNotFoundError:
                pass
        os.rmdir(os.path.dirname(cache_entry['cache_path']))
    
    def clear_all(self):
        """
        Clears all cache and metadata.
        """
        for urlpath in self._metadata.keys():
            self.clear_cache(urlpath)
        
        # Safely clean up anything else.
        try:
            for subdir in os.listdir(self._cache_dir):
                shutil.rmtree(os.path.join(self._cache_dir, subdir))
        except FileNotFoundError:
            pass


def _download(file_in, file_out, blocksize, output=False):
    """Read from input and write to output file in blocks"""
    if output:
        from tqdm.autonotebook import tqdm

        try:
            file_size = file_in.fs.size(file_in.path)
            pbar_disabled = False
        except ValueError as err:
            logger.debug("File system error requesting size: {}".format(err))
            pbar_disabled = True
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

    logger.debug("Caching {}".format(file_in.path))
    with file_in as f1:
        with file_out as f2:
            data = True
            while data:
                data = f1.read(blocksize)
                f2.write(data)
                if output:
                    pbar.update(len(data) // 2**20)
    if output:
        pbar.update(pbar.total - pbar.n)  # force to full
        pbar.close()
        display.remove(out)


class FileCache(BaseCache):
    def _make_files(self, urlpath):
        from dask.bytes import open_files

        self._ensure_cache_dir()
        subdir = self._hash(urlpath)
        files_in = open_files(urlpath, 'rb')
        files_out = [open_files([self._path(f.path, subdir)], 'wb')[0]
                     for f in files_in]
        return files_in, files_out


class DirCache(BaseCache):
    def _make_files(self, urlpath, **kwargs):
        from dask.bytes import open_files

        self._ensure_cache_dir()
        subdir = self._hash(urlpath)
        depth = self._spec['depth']
        files_in = []
        for i in range(1, depth + 1):
            files_in.extend(open_files('/'.join([urlpath] + ['*']*i)))
        files_out = [open_files([self._path(f.path, subdir)], 'wb')[0]
                     for f in files_in]
        files_in2, files_out2 = [], []
        paths = set(os.path.dirname(f.path) for f in files_in)
        print(paths)
        for fin, fout in zip(files_in, files_out):
            print(fin, fout)
            if fin.path in paths:
                try:
                    print('mkdir', fout.path)
                    os.makedirs(fout.path)
                except Exception:
                    pass
            else:
                files_in2.append(fin)
                files_out2.append(fout)
        return files_in2, files_out2


class CompressedCache(BaseCache):
    def _make_files(self, urlpath, **kwargs):
        import dask.bytes.compression
        import tempfile
        ext = os.path.splitext(urlpath)[1]
        comp = kwargs.get('compression', ext)


class CacheMetadata(collections.MutableMapping):
    """
    Utility class for managing persistent metadata stored in the Intake config directory.
    """
    def __init__(self, *args, **kwargs):
        from intake import config

        self._path = os.path.join(config.confdir, 'cache_metadata.json')

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
        return key

    def update(self, key, cache_entry):
        entries = self._metadata.get(key, [])
        entries.append(cache_entry)
        self._metadata[key] = entries
        self._save()

    def _save(self):
        with open(self._path, 'w') as f:
            json.dump(self._metadata, f)

    def pop(self, key):
        item = self._metadata.pop(key)
        self._save()
        return item
    
    def keys(self):
        return list(self._metadata.keys())


registry = {
    'file': FileCache,
    'dir': DirCache,
    'copmressed': CompressedCache
}


def make_caches(driver, specs):
    """
    Creates Cache objects from the cache_specs provided in the catalog yaml file.
    
    Parameters:
    -----------
    driver: str
        Name of the plugin that can load catalog entry
    specs: list
        Specification for caching the data source.
    """
    if specs is None:
        return []
    return [registry.get(spec['type'], FileCache)(driver, spec) for spec in specs]
