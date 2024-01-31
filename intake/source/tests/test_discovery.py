# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import os.path
import shlex
import subprocess
import sys

import pytest

import intake
from intake.source import discovery


@pytest.fixture
def extra_pythonpath():
    basedir = os.path.dirname(__file__)
    extra_path = os.path.join(basedir, "plugin_searchpath")

    # Put extra directory on the python path
    sys.path.append(extra_path)

    yield extra_path

    # Return python path back to normal
    sys.path.remove(extra_path)


def test_package_scan(extra_pythonpath, tmp_config_path):
    "This tests a non-public function."
    # Default path (sys.path)
    results = discovery._package_scan()
    assert "foo" in results

    # Explicit path
    results = discovery._package_scan(path=[extra_pythonpath])
    assert "foo" in results


def test_discover_cli(extra_pythonpath, tmp_config_path):
    env = os.environ.copy()
    env["INTAKE_CONF_FILE"] = tmp_config_path
    env["PYTHONPATH"] = extra_pythonpath

    # directory is not automatically scanned any more
    subprocess.call(
        shlex.split("intake drivers enable foo intake_foo.FooPlugin"),
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        env=env,
    )

    out = subprocess.check_output(
        shlex.split("intake drivers list"), stderr=subprocess.STDOUT, env=env
    )

    assert b"foo" in out
    assert out.index(b"Disabled") > out.index(b"foo")

    subprocess.check_output(
        shlex.split("intake drivers disable foo"), stderr=subprocess.STDOUT, env=env
    )

    out = subprocess.check_output(
        shlex.split("intake drivers list"), stderr=subprocess.STDOUT, env=env
    )
    assert b"foo" in out
    assert out.index(b"Disabled") < out.index(b"foo")


def test_discover(extra_pythonpath, tmp_config_path):
    drivers = intake.source.discovery.DriverSouces(do_scan=True)
    with pytest.warns(PendingDeprecationWarning):
        assert "foo" in drivers.scanned

    registry = intake.source.DriverRegistry(drivers)
    # Check that package scan (name-based) discovery worked.
    assert "foo" in registry
    registry["foo"]()
    # Check that entrypoints-based discovery worked.
    assert "some_test_driver" in registry
    registry["some_test_driver"]()

    # Now again, turning off the package scan.

    drivers = intake.source.discovery.DriverSouces()
    registry = intake.source.DriverRegistry(drivers)

    # Check that package scan (name-based) discovery did *not* happen.
    assert "foo" not in registry
    # Check that entrypoints-based discovery worked.
    assert "some_test_driver" in registry
    registry["some_test_driver"]()


def test_enable_and_disable(extra_pythonpath, tmp_config_path):
    # Disable and then enable a package scan result.

    try:
        drivers = intake.source.discovery.DriverSouces(do_scan=True)
        registry = intake.source.DriverRegistry(drivers)
        assert "foo" in registry

        drivers.disable("foo")
        with pytest.warns(PendingDeprecationWarning):
            assert "foo" in discovery.drivers.scanned
        assert "foo" not in registry

        drivers.enable("foo", "intake_foo.FooPlugin")
        assert "foo" in registry
    finally:
        drivers.enable("foo", "intake_foo.FooPlugin")

    # Disable and then enable an entrypoint result.

    try:
        drivers.disable("some_test_driver")
        assert "some_test_driver" not in registry

        drivers.enable("some_test_driver", "driver_with_entrypoints.SomeTestDriver")
        assert "some_test_driver" in registry
    finally:
        drivers.enable("some_test_driver", "driver_with_entrypoints.SomeTestDriver")


def test_register_and_unregister(extra_pythonpath, tmp_config_path):
    registry = intake.source.registry
    assert "bar" not in registry
    with pytest.raises(ImportError):
        from intake import open_bar

    intake.register_driver("bar", "intake_foo.FooPlugin")
    assert "bar" in registry
    from intake import open_bar  # noqa

    intake.unregister_driver("bar")
    assert "bar" not in registry

    with pytest.raises(ImportError):
        from intake import open_bar  # noqa


def test_discover_collision(extra_pythonpath, tmp_config_path):
    with pytest.warns(UserWarning):
        discovery._package_scan(plugin_prefix="collision_")
