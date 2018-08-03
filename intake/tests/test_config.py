import os
import pytest
import requests

import intake
from intake import config
from intake.util_tests import temp_conf, server


@pytest.mark.parametrize('conf', [
    {},
    {'port': 5000},
    {'other': True}
])
def test_load_conf(conf):
    inconf = config.conf.copy()
    expected = inconf.copy()
    with temp_conf(conf) as fn:
        config.load_conf(fn)
        expected.update(conf)
        assert config.conf == expected
        config.reset_conf()
        assert config.conf == inconf


# Tests with a real separate server process


def test_basic():
    with temp_conf({}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(env=env, wait=5000):
            r = requests.get('http://localhost:5000/v1/info')
            assert r.ok


def test_cli():
    with temp_conf({}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(args=('-p', '5555'), env=env, wait=5555):
            r = requests.get('http://localhost:5555/v1/info')
            assert r.ok


def test_conf():
    with temp_conf({'port': 5555}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(env=env, wait=5555):
            r = requests.get('http://localhost:5555/v1/info')
            assert r.ok


def test_conf_auth():
    with temp_conf({'auth': {'class': 'intake.auth.secret.SecretAuth',
                             'kwargs': {'secret': 'test'}}}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(env=env, wait=5000):
            # raw request
            r = requests.get('http://localhost:5000/v1/info')
            assert r.status_code == 403
            r = requests.get('http://localhost:5000/v1/info',
                             headers={'intake-secret': 'test'})
            assert r.ok

            # with cat
            with pytest.raises(Exception):
                intake.Catalog('intake://localhost:5000')

            cat = intake.Catalog('intake://localhost:5000',
                                 storage_options={'headers':
                                                  {'intake-secret': 'test'}})
            assert 'entry1' in cat
