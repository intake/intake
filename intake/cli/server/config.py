
import os
import yaml

confdir = os.getenv('INTAKE_CONF_DIR',
                    os.path.join(os.path.expanduser('~'), '.intake'))


defaults = {'auth': {'class': 'intake.auth.base.BasicAuth'},
            'port': 5000}
conf = defaults.copy()


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


load_conf()
