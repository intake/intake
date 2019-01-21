#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from os.path import expanduser
import logging
import os
import posixpath
import yaml
from .utils import make_path_posix
logger = logging.getLogger('intake')

confdir =  make_path_posix(
    os.getenv('INTAKE_CONF_DIR', os.path.join(expanduser('~'), '.intake')))


defaults = {
    'auth': {'class': 'intake.auth.base.BaseAuth'},
    'port': 5000,
    'cache_dir': posixpath.join(confdir, 'cache'),
    'cache_disabled': False,
    'cache_download_progress': True,
    'logging': 'INFO',
    'catalog_path': []
    }
conf = {}


def cfile():
    return make_path_posix(
        os.getenv('INTAKE_CONF_FILE', posixpath.join(confdir, 'conf.yaml')))


def reset_conf():
    """Set conf values back to defaults"""
    conf.clear()
    conf.update(defaults)


def save_conf(fn=None):
    """Save current configuration to file as YAML

    If not given, uses current config directory, ``confdir``, which can be
    set by INTAKE_CONF_DIR.
    """
    if fn is None:
        fn = cfile()
    try:
        os.makedirs(os.path.dirname(fn))
    except (OSError, IOError):
        pass
    with open(fn, 'w') as f:
        yaml.dump(conf, f)


def load_conf(fn=None):
    """Update global config from YAML file

    If fn is None, looks in global config directory, which is either defined
    by the INTAKE_CONF_DIR env-var or is ~/.intake/ .
    """
    if fn is None:
        fn = cfile()
    if os.path.isfile(fn):
        with open(fn) as f:
            try:
                conf.update(yaml.load(f))
            except Exception as e:
                logger.warning('Failure to load config file "{fn}": {e}'
                               ''.format(fn=fn, e=e))


def intake_path_dirs(path):
    """Return a list of directories from the intake path."""
    separator = ';' if os.name == 'nt' else ':'
    return path.split(separator)


reset_conf()
load_conf(cfile())
# environment variables take precedence over conf file
if 'INTAKE_CACHE_DIR' in os.environ:
    conf['cache_dir'] = make_path_posix(os.environ['INTAKE_CACHE_DIR'])
if 'INTAKE_DISABLE_CACHING' in os.environ:
    conf['cache_disabled'] = os.environ['INTAKE_DISABLE_CACHING'
                                        ].lower() == 'true'
if 'INTAKE_CACHE_PROGRESS' in os.environ:
    conf['cache_download_progress'] = os.environ['INTAKE_CACHE_PROGRESS'
                                      ].lower() == 'true'
if 'INTAKE_LOG_LEVEL' in os.environ:
    conf['logging'] = os.environ['INTAKE_LOG_LEVEL']
if 'INTAKE_PATH' in os.environ:
    conf['catalog_path'] = make_path_posix(
        intake_path_dirs(os.environ['INTAKE_PATH']))
logger.setLevel(conf['logging'])
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s:%(levelname)s, %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.debug('Intake logger set to debug')
