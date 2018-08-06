from datetime import datetime
from hashlib import md5
from pathlib import Path

import json
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

    def _path(self, urlpath):
        import re
        cache_path = re.sub(
            r"%s" % self._spec['regex'],
            self._cache_dir,
            urlpath
        )
        filename = md5(str((os.path.basename(cache_path), self._driver)).encode()).hexdigest()
        dirname = os.path.dirname(cache_path)
        return filename, os.path.join(dirname, filename)

    def load(self, urlpath):
        import urllib.request

        cache_name, cache_path = self._path(urlpath)

        if not os.path.isfile(cache_path):
            print("Caching file from {}".format(urlpath))
            self._metadata[cache_name] = {
                'created': datetime.now().isoformat(),
                'urlpath': urlpath
            }
            urllib.request.urlretrieve(urlpath, cache_path)

        return cache_path

class CacheMetadata(object):

    def __init__(self, cache_dir):
        self._cache_dir = cache_dir
        self._path = os.path.join(self._cache_dir, 'metadata.json')

        if os.path.isfile(self._path):
            with open(self._path) as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {}
    
    def __setitem__(self, key, item):
        self._metadata[key] = item
        with open(self._path, 'w') as f:
            json.dump(self._metadata, f)
    
    def __getitem__(self, key):
        return self._metadata[key]