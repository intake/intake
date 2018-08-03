from hashlib import md5
from pathlib import Path

import os.path

def parse_cache_specs(driver, cache_specs):
    if cache_specs is None:
        return []
    return [Cache(driver, spec) for spec in cache_specs]

class Cache(object):

    def __init__(self, driver, spec):
        self._driver = driver
        self._spec = spec
        #TODO: Need to allow override by env var and config file.
        self._cache_dir = "{}/.intake/cache".format(str(Path.home()))
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        import os
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
        return os.path.join(dirname, filename)

    def load(self, urlpath):
        import urllib.request

        cache_path = self._path(urlpath)

        if not os.path.isfile(cache_path):
            print("Downloading file from {}".format(urlpath))
            urllib.request.urlretrieve(urlpath, cache_path)

        return cache_path
