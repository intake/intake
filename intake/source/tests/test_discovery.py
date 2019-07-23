#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import os.path
import sys

import pytest

from intake.source import discovery
from intake.config import cfile


@pytest.fixture
def config(tmp_path):
    key = 'INTAKE_CONF_FILE'
    original = os.getenv(key)
    temp_config_path = os.path.join(tmp_path, 'test_config.yml')
    os.environ[key] = temp_config_path
    assert cfile() == temp_config_path
    yield temp_config_path
    if original:
        os.environ[key] = original


@pytest.fixture
def extra_pythonpath():
    basedir = os.path.dirname(__file__)
    extra_path = os.path.join(basedir, 'plugin_searchpath')

    # Put extra directory on the python path
    sys.path.append(extra_path)

    yield extra_path

    # Return python path back to normal
    sys.path.remove(extra_path)


def test_package_scan(extra_pythonpath, config):
    "This tests a non-public function."
    # Default path (sys.path)
    results = discovery._package_scan()
    assert 'foo' in results

    # Explicit path
    results = discovery._package_scan(path=[extra_pythonpath])
    assert 'foo' in results


def test_discover(extra_pythonpath, config):
    with pytest.warns(PendingDeprecationWarning):
        registry = discovery.autodiscover()

    # Check that package scan (name-based) discovery worked.
    assert 'foo' in registry
    registry['foo']()
    # Check that entrypoints-based discovery worked.
    assert 'some_test_driver' in registry
    registry['some_test_driver']()

    # Now again, giving the special PYTHONPATH explicit via kwarg.

    with pytest.warns(PendingDeprecationWarning):
        registry = discovery.autodiscover(path=[extra_pythonpath])

    # Check that package scan (name-based) discovery worked.
    assert 'foo' in registry
    registry['foo']()
    # Check that entrypoints-based discovery worked.
    assert 'some_test_driver' in registry
    registry['some_test_driver']()

    # Now again, turning off the package scan.

    registry = discovery.autodiscover(
        path=[extra_pythonpath],
        do_package_scan=False)

    # Check that package scan (name-based) discovery did *not* happen.
    assert 'foo' not in registry
    # Check that entrypoints-based discovery worked.
    assert 'some_test_driver' in registry
    registry['some_test_driver']()

def test_enable_and_disable(extra_pythonpath, config):
    # Disable and then enable a package scan result.

    try:
        discovery.disable('foo')
        with pytest.warns(PendingDeprecationWarning):
            registry = discovery.autodiscover()
        assert 'foo' not in registry

        discovery.enable('foo', 'intake_foo.FooPlugin')
        with pytest.warns(PendingDeprecationWarning):
            registry = discovery.autodiscover()
        assert 'foo' in registry
    finally:
        discovery.enable('foo', 'intake_foo.FooPlugin')

    # Disable and then enable an entrypoint result.

    try:
        discovery.disable('some_test_driver')
        with pytest.warns(PendingDeprecationWarning):
            registry = discovery.autodiscover()
        assert 'some_test_driver' not in registry

        discovery.enable(
            'some_test_driver',
            'driver_with_entrypoints.SomeTestDriver')
        with pytest.warns(PendingDeprecationWarning):
            registry = discovery.autodiscover()
        assert 'some_test_driver' in registry
    finally:
        discovery.enable(
            'some_test_driver',
            'driver_with_entrypoints.SomeTestDriver')



def test_discover_pluginprefix(extra_pythonpath, config):
    with pytest.warns(PendingDeprecationWarning):
        registry = discovery.autodiscover(plugin_prefix='not_intake_')

    assert 'otherfoo' in registry
    registry['otherfoo']()
    registry.pop('otherfoo', None)


def test_discover_collision(extra_pythonpath, config):
    with pytest.warns(UserWarning):
        discovery.autodiscover(plugin_prefix='collision_')
