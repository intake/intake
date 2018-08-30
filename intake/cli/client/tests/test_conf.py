import os
import subprocess
import pytest


def test_reset(tmpdir):
    subprocess.call('INTAKE_CONF_DIR=%s intake config reset' % tmpdir,
                    shell=True)
    fn = os.path.join(tmpdir, 'conf.yaml')
    assert os.path.isfile(fn)
    txt = open(fn).read()
    assert 'port: 5000' in txt


def test_info(tmpdir):
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake config info' %
                                  tmpdir, shell=True).decode()
    fn = os.path.join(tmpdir, 'conf.yaml')
    assert fn in out
    assert 'INTAKE_CONF_DIR' in out
    assert 'INTAKE_CONF_FILE' not in out
    assert "(does not exist)" in out
    with open(fn, 'w') as f:
        f.write('port: 5000')
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake config info' %
                                  tmpdir, shell=True).decode()
    assert "(does not exist)" not in out


def test_defaults(tmpdir):
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake config list-'
                                  'defaults' % tmpdir, shell=True).decode()
    assert 'port: 5000' in out


def test_get(tmpdir):
    fn = os.path.join(tmpdir, 'conf.yaml')
    with open(fn, 'w') as f:
        f.write('port: 5001')
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake config get' %
                                  tmpdir, shell=True).decode()
    assert 'port: 5001' in out
    out = subprocess.check_output('INTAKE_CONF_DIR=%s intake config get port' %
                                  tmpdir, shell=True).decode()
    assert out == '5001\n'


def test_log_level():
    out = subprocess.check_output('INTAKE_LOG_LEVEL=DEBUG intake config info',
                                  shell=True, stderr=subprocess.STDOUT).decode()
    assert "logger set to debug" in out
