#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import posixpath
import subprocess


def test_reset(env):
    subprocess.call('intake config reset', env=env)
    confdir = env['INTAKE_CONF_DIR']
    fn = posixpath.join(confdir, 'conf.yaml')
    assert os.path.isfile(fn)
    txt = open(fn).read()
    assert 'port: 5000' in txt


def test_info(env):
    out = subprocess.check_output('intake config info', env=env).decode()
    confdir = env['INTAKE_CONF_DIR']
    fn = posixpath.join(confdir, 'conf.yaml')
    assert fn in out
    assert 'INTAKE_CONF_DIR' in out
    assert 'INTAKE_CONF_FILE' not in out
    assert "(does not exist)" in out
    with open(fn, 'w') as f:
        f.write('port: 5000')
    out = subprocess.check_output('intake config info', env=env).decode()
    assert "(does not exist)" not in out


def test_defaults(env):
    out = subprocess.check_output('intake config list-defaults', 
                                  env=env).decode()
    assert 'port: 5000' in out


def test_get(env):
    confdir = env['INTAKE_CONF_DIR']
    fn = posixpath.join(confdir, 'conf.yaml')
    with open(fn, 'w') as f:
        f.write('port: 5001')
    out = subprocess.check_output('intake config get', env=env).decode()
    assert 'port: 5001' in out
    out = subprocess.check_output('intake config get port', env=env).decode()
    assert out.startswith('5001')


def test_log_level():
    env = os.environ.copy()
    env['INTAKE_LOG_LEVEL'] = 'DEBUG'
    out = subprocess.check_output('intake config info',
                                  stderr=subprocess.STDOUT, env=env).decode()
    assert "logger set to debug" in out
