import appdirs
import json
import os
import subprocess

from .base import Catalog


def load_user_catalog():
    """Return a catalog for the platform-specific user Intake directory"""
    cat_dir = user_data_dir()
    if not os.path.isdir(cat_dir):
        return Catalog()
    else:
        return Catalog(cat_dir)


def user_data_dir():
    """Return the user Intake catalog directory"""
    return appdirs.user_data_dir(appname='intake', appauthor='intake')


def load_global_catalog():
    """Return a catalog for the environment-specific Intake directory"""
    cat_dir = global_data_dir()
    if not os.path.isdir(cat_dir):
        return Catalog()
    else:
        return Catalog(cat_dir)


CONDA_VAR = 'CONDA_PREFIX'
VIRTUALENV_VAR = 'VIRTUAL_ENV'


def conda_prefix():
    """Fallback: ask conda in PATH for its prefix"""
    try:
        out = subprocess.check_output(['conda', 'info', '--json'])
        return json.loads(out.decode())["default_prefix"]
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError):
        return False


def global_data_dir():
    """Return the global Intake catalog dir for the current environment"""

    if VIRTUALENV_VAR in os.environ:
        prefix = os.environ[VIRTUALENV_VAR]
    else:
        prefix = conda_prefix()
    
    if prefix:
        # conda and virtualenv use Linux-style directory pattern
        return os.path.join(prefix, 'share', 'intake')
    else:
        return appdirs.site_data_dir(appname='intake', appauthor='intake')


def load_combo_catalog():
    """Load a union of the user and global catalogs for convenience"""
    user_dir = user_data_dir()
    global_dir = global_data_dir()

    cat_dirs = []
    if os.path.isdir(user_dir):
        cat_dirs.append(user_dir)
    if os.path.isdir(global_dir):
        cat_dirs.append(global_dir)

    # TODO: if we find no dirs or dirs are empty,
    # Catalog should cope and return empty without this branching
    if len(cat_dirs) > 0:
        return Catalog(cat_dirs)
    else:
        return Catalog()
