"""Intake config manipulations and persistence"""

# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import ast
import contextlib
import copy
import logging
import os
import posixpath
from os.path import expanduser

import yaml
from fsspec.implementations.local import make_path_posix

from intake.utils import yaml_load

logger = logging.getLogger("intake")

confdir = make_path_posix(os.getenv("INTAKE_CONF_DIR", os.path.join(expanduser("~"), ".intake")))


defaults = {
    "logging": "INFO",
    "catalog_path": [],
    "allow_import": True,
    "allow_templates": True,
    "allow_pickle": False,
    "import_on_startup": True,
    "extra_imports": [],
    "import_block_list": [],
    "reader_avoid": [],
}


def cfile():
    return make_path_posix(os.getenv("INTAKE_CONF_FILE", posixpath.join(confdir, "conf.yaml")))


class Config(dict):
    """Intake's dict-like config system

    Instance ``intake.conf`` is globally used throughout the package
    """

    def __init__(self, filename=None, **kwargs):
        self.filename = filename if filename is not None else cfile()
        self.reload_all()
        self.temp = None
        super().__init__(**kwargs)
        logger.setLevel(self["logging"])

    def reset(self):
        """Set conf values back to defaults"""
        self.clear()
        self.update(defaults)

    def save(self, fn=None):
        """Save current configuration to file as YAML

        Uses ``self.filename`` for target location
        """
        # TODO: fsspec?
        fn = fn or self.filename
        if fn is False:
            return
        try:
            os.makedirs(os.path.dirname(fn))
        except (OSError, IOError):
            pass
        with open(fn, "w") as f:
            yaml.dump(dict(self), f)

    @contextlib.contextmanager
    def _unset(self, temp):
        yield
        self.clear()
        self.update(temp)

    def set(self, update_dict=None, **kw):
        """Change config values within a context or for the session

        values: dict
            This can be deeply nested to set only leaf values

        See also: ``intake.readers.utils.nested_keys_to_dict``

        Examples
        --------

        Value resets after context ends

        >>> with intake.conf.set(mybval=5):
        ...     ...

        Set for whole session

        >>> intake.conf.set(myval=5)

        Set only a single leaf value within a nested dict

        >>> intake.conf.set(intake.readers.utils.nested_keys_to_dict({"deep.2.key": True})
        """
        temp = copy.deepcopy(self)
        if update_dict:
            kw.update(update_dict)
        from intake.readers.utils import merge_dicts

        self.update(merge_dicts(self, kw))
        return self._unset(temp)

    def __getitem__(self, item):
        if item in self:
            return super().__getitem__(item)
        elif item in defaults:
            return defaults[item]
        else:
            raise KeyError(item)

    def get(self, key, default=None):
        if key in self:
            return super().__getitem__(key)
        return default

    def reload_all(self):
        self.reset()
        self.load()
        self.load_env()

    def load(self, fn=None):
        """Update global config from YAML file

        If fn is None, looks in global config directory, which is either defined
        by the INTAKE_CONF_DIR env-var or is ~/.intake/ .
        """
        # TODO: if fn is not None, set self.filename
        # TODO: fsspec?
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
        for k, v in os.environ.items():
            if k.startswith("INTAKE_"):
                k2 = k[7:].lower()
                try:
                    val = ast.literal_eval(v)
                except ValueError:
                    pass
                self[k2] = val


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
