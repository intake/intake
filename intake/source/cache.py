from datetime import datetime
from hashlib import md5
from pathlib import Path

import json
import shutil
import os

from intake.config import conf

def parse_cache_specs(driver, cache_specs):
    if cache_specs is None:
        return []
    return [Cache(driver, spec) for spec in cache_specs]

class Cache(object):

    def __init__(self, driver, spec):
        self._driver = driver
        self._spec = spec
        self._cache_dir = os.getenv('INTAKE_CACHE_DIR',
                                    conf['cache_dir'])
                             
        self._ensure_cache_dir()
        self._metadata = CacheMetadata(self._cache_dir)
    
    def _ensure_cache_dir(self):
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)

    def _munge_path(self, urlpath):
        import re
        cache_path = re.sub(
            r"%s" % self._spec['regex'],
            self._cache_dir,
            urlpath
        )
        return cache_path

    def _path(self, urlpath):
        cache_path = self._munge_path(urlpath)
        filename = md5(str((os.path.basename(cache_path), self._driver)).encode()).hexdigest()
        dirname = os.path.dirname(cache_path)
        return os.path.join(dirname, filename)

    def _log_metadata(self, urlpath, original_path, cache_path):
        metadata = {
            'created': datetime.now().isoformat(),
            'original_path': original_path,
            'cache_path': cache_path
            }
        self._metadata.update(urlpath, metadata)

    def load(self, urlpath):
        BLOCKSIZE = 5000000
        from dask.bytes import open_files

        cache_paths = []
        files_in = open_files(urlpath, 'rb')
        files_out = open_files([self._path(f.path) for f in files_in], 'wb')
        for file_in, file_out in zip(files_in, files_out):
            cache_path = file_out.path
            cache_paths.append(cache_path)

            if not os.path.isfile(cache_path):
                print("Caching file from {}".format(urlpath))
                self._log_metadata(urlpath, file_in.path, cache_path)

                with file_in as f1:
                    with file_out as f2:
                        data = True
                        while data:
                            #TODO: print out progress
                            data = f1.read(BLOCKSIZE)
                            f2.write(data)
        return cache_paths

    def get_metadata(self, urlpath):
        return self._metadata[urlpath]
    
    def clear_cache(self, urlpath):
        cache_entries = self._metadata.pop(urlpath)
        for cache_entry in cache_entries:
            os.remove(cache_entry['cache_path'])
    
    def clear_all(self):
        shutil.rmtree(self._cache_dir)

class CacheMetadata(object):

    def __init__(self, cache_dir):
        self._cache_dir = cache_dir
        self._path = os.path.join(self._cache_dir, 'metadata.json')

        if os.path.isfile(self._path):
            with open(self._path) as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {}
    
    def update(self, key, cache_entry):
        entries = self._metadata.get(key, [])
        entries.append(cache_entry)
        self._metadata[key] = entries
        self._save()

    def _save(self):
        with open(self._path, 'w') as f:
            json.dump(self._metadata, f)

    def __setitem__(self, key, item):
        self._metadata[key] = item
        self._save()
    
    def __getitem__(self, key):
        return self._metadata[key]

    def pop(self, key):
        item = self._metadata.pop(key)
        self._save()
        return item