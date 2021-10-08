#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
import requests

import intake
from intake import config
from intake.util_tests import temp_conf, server
from intake.catalog.remote import RemoteCatalog

@pytest.mark.parametrize('conf', [
    {},
    {'port': 5000},
    {'other': True}
])
def test_load_conf(conf):
    # This test will only work if your config is set to default
    inconf = config.defaults.copy()
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
    with temp_conf({}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF'] = os.path.dirname(fn)
        with server(env=env, wait=5000):
            r = requests.get('http://localhost:5000/v1/info')
            assert r.ok
    with temp_conf({}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF'] = os.path.dirname(fn) + ":/nonexistent"
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


def test_persist_modes():
    expected_never = "never"
    expected_default = "default"

    with temp_conf({}) as fn:
        env = os.environ.copy()
        env["INTAKE_CONF_FILE"] = fn
        with server(args=("-p", "5555"), env=env, wait=5555):
            cat_never = RemoteCatalog("intake://localhost:5555", persist_mode="never")
            assert cat_never.pmode == expected_never

            cat_default = RemoteCatalog("intake://localhost:5555")
            assert cat_default.pmode == expected_default

def test_conf():
    with temp_conf({'port': 5555}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(env=env, wait=5555):
            r = requests.get('http://localhost:5555/v1/info')
            assert r.ok


def test_conf_auth():
    with temp_conf({'port': 5556,
                    'auth': {'cls': 'intake.auth.secret.SecretAuth',
                             'kwargs': {'secret': 'test'}}}) as fn:
        env = os.environ.copy()
        env['INTAKE_CONF_FILE'] = fn
        with server(env=env, wait=5556):
            # raw request
            r = requests.get('http://localhost:5556/v1/info')
            assert r.status_code == 403
            r = requests.get('http://localhost:5556/v1/info',
                             headers={'intake-secret': 'test'})
            assert r.ok

            # with cat
            with pytest.raises(Exception):
                intake.open_catalog('intake://localhost:5556')

            cat = intake.open_catalog('intake://localhost:5556',
                                      storage_options={'headers':
                                                       {'intake-secret': 'test'}})
            assert 'entry1' in cat


@pytest.mark.skipif(os.name == 'nt', reason="Paths are different on win")
def test_pathdirs():
    assert config.intake_path_dirs([]) == []
    assert config.intake_path_dirs(['paths']) == ['paths']
    assert config.intake_path_dirs("") == [""]
    assert config.intake_path_dirs("path1:path2") == ['path1', 'path2']
    assert config.intake_path_dirs("memory://path1:memory://path2") == [
        'memory://path1', 'memory://path2']
