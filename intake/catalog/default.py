import appdirs
import json
import os
import subprocess
import sys

from .local import YAMLFilesCatalog, Catalog


def load_user_catalog():
    """Return a catalog for the platform-specific user Intake directory"""
    cat_dir = user_data_dir()
    if not os.path.isdir(cat_dir):
        return Catalog()
    else:
        return YAMLFilesCatalog(cat_dir)


def user_data_dir():
    """Return the user Intake catalog directory"""
    return appdirs.user_data_dir(appname='intake', appauthor='intake')


def load_global_catalog():
    """Return a catalog for the environment-specific Intake directory"""
    cat_dir = global_data_dir()
    if not os.path.isdir(cat_dir):
        return Catalog()
    else:
        return YAMLFilesCatalog(cat_dir)


CONDA_VAR = 'CONDA_PREFIX'
VIRTUALENV_VAR = 'VIRTUAL_ENV'


def conda_prefix():
    """Fallback: ask conda in PATH for its prefix"""
    try:
        out = subprocess.check_output(['conda', 'info', '--json'])
        return json.loads(out.decode())["default_prefix"]
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError):
        return False


def which(program):
    """Emulate posix ``which``

    https://stackoverflow.com/a/377028/3821154
    """
    import os

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


def global_data_dir():
    """Return the global Intake catalog dir for the current environment"""
    prefix = False
    if VIRTUALENV_VAR in os.environ:
        prefix = os.environ[VIRTUALENV_VAR]
    elif CONDA_VAR in os.environ:
        prefix = sys.prefix
    elif which('conda'):
        # conda exists but is not activated
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
        cat_dirs.append(user_dir + '/*.yaml')
        cat_dirs.append(user_dir + '/*.yml')
    if os.path.isdir(global_dir):
        cat_dirs.append(global_dir + '/*.yaml')
        cat_dirs.append(global_dir + '/*.yml')

    return YAMLFilesCatalog(cat_dirs, name='builtin')
