
from os.path import expanduser

import os
import yaml

confdir = os.getenv('INTAKE_CONF_DIR',
                    os.path.join(expanduser('~'), '.intake'))
conffile = os.getenv('INTAKE_CONF_FILE', None)


defaults = {
    'auth': {'class': 'intake.auth.base.BaseAuth'},
    'port': 5000,
    'cache_dir': os.path.join(expanduser('~'), '.intake/cache'),
    'cache_disabled': os.environ.get('INTAKE_DISABLE_CACHING', False)
    }
conf = {}


def reset_conf():
    """Set conf values back to defaults"""
    conf.clear()
    conf.update(defaults)


def load_conf(fn=None):
    """Update global config from YAML file

    If fn is None, looks in global config directory, which is either defined
    by the INTAKE_CONF_DIR env-var or is ~/.intake/ .
    """
    if fn is None:
        fn = os.path.join(confdir, 'conf.yaml')
    if os.path.isfile(fn):
        with open(fn) as f:
            conf.update(yaml.load(f))


reset_conf()
load_conf(conffile)
