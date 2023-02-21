# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import logging
import os
import posixpath
from os.path import expanduser

import yaml

from intake.utils import yaml_load

from .utils import make_path_posix

logger = logging.getLogger("intake")

confdir = make_path_posix(os.getenv("INTAKE_CONF_DIR", os.path.join(expanduser("~"), ".intake")))


defaults = {
    "auth": {"cls": "intake.auth.base.BaseAuth"},
    "port": 5000,
    "cache_dir": posixpath.join(confdir, "cache"),
    "cache_disabled": False,
    "cache_download_progress": True,
    "logging": "INFO",
    "catalog_path": [],
    "persist_path": posixpath.join(confdir, "persisted"),
    "package_scan": False,
}


def cfile():
    return make_path_posix(os.getenv("INTAKE_CONF_FILE", posixpath.join(confdir, "conf.yaml")))


class Config(dict):
    def __init__(self, filename=None, **kwargs):
        self.filename = filename if filename is not None else cfile()
        self.reload_all()
        super().__init__(**kwargs)

    def reset(self):
        """Set conf values back to defaults"""
        self.clear()
        self.update(defaults)

    def save(self):
        """Save current configuration to file as YAML

        Uses ``.filename`` for target location
        """
        if self.filename is False:
            return
        try:
            os.makedirs(os.path.dirname(self.filename))
        except (OSError, IOError):
            pass
        with open(self.filename, "w") as f:
            yaml.dump(dict(self), f)

    def reload_all(self):
        self.reset()
        self.load()
        self.load_env()

    def load(self, fn=None):
        """Update global config from YAML file

        If fn is None, looks in global config directory, which is either defined
        by the INTAKE_CONF_DIR env-var or is ~/.intake/ .
        """
        fn = fn or self.filename

        if os.path.isfile(fn):
            with open(fn) as f:
                try:
                    self.update(yaml_load(f))
                except Exception as e:
                    logger.warning('Failure to load config file "{fn}": {e}' "".format(fn=fn, e=e))

    def load_env(self):
        """Analyse environment variables and update conf accordingly"""
        # environment variables take precedence over conf file
        for key, envvar in [["cache_dir", "INTAKE_CACHE_DIR"], ["catalog_path", "INTAKE_PATH"], ["persist_path", "INTAKE_PERSIST_PATH"]]:
            if envvar in os.environ:
                self[key] = make_path_posix(os.environ[envvar])
        self["catalog_path"] = intake_path_dirs(self["catalog_path"])
        for key, envvar in [["cache_disabled", "INTAKE_DISABLE_CACHING"], ["cache_download_progress", "INTAKE_CACHE_PROGRESS"]]:
            if envvar in os.environ:
                self[key] = os.environ[envvar].lower() in ["true", "t", "y", "yes"]
        if "INTAKE_LOG_LEVEL" in os.environ:
            self["logging"] = os.environ["INTAKE_LOG_LEVEL"]


def intake_path_dirs(path):
    """Return a list of directories from the intake path.

    If a string, perhaps taken from an environment variable, then the
    list of paths will be split on the character ":" for posix of ";" for
    windows. Protocol indicators ("protocol://") will be ignored.
    """
    if isinstance(path, (list, tuple)):
        return path
    import re

    pattern = re.compile(";" if os.name == "nt" else r"(?<!:):(?![:/])")
    return pattern.split(path)


conf = Config()
conf.reload_all()
save_conf = conf.save
load_cond = conf.load

logger.setLevel(conf["logging"].upper())
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - " "%(filename)s:%(funcName)s:L%(lineno)d - " "%(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.debug("Intake logger set to debug")
