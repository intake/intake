from pathlib import Path

def parse_cache_specs(cache_specs):
    if cache_specs is None:
        return []
    return [Cache(spec) for spec in cache_specs]

class Cache(object):

    def __init__(self, spec):
        self._spec = spec
        #TODO: Need to allow override by env var and config file.
        self._cache_dir = "{}/.intake/cache".format(str(Path.home()))
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        import os
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)

    def _path(self, urlpath):
        #TODO: generate hash for cache file name based on driver and path.
        import re
        return re.sub(
            r"%s" % self._spec['regex'],
            self._cache_dir,
            urlpath
        )

    def load(self, urlpath):
        import urllib.request
        import os.path

        cache_path = self._path(urlpath)

        if not os.path.isfile(cache_path):
            print("Downloading file from {}".format(urlpath))
            urllib.request.urlretrieve(urlpath, cache_path)

        return cache_path
