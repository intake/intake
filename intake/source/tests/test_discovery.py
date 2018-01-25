import os
import sys

import pytest

from intake.source import discovery


@pytest.fixture
def extra_pythonpath():
    basedir = os.path.dirname(__file__)
    extra_path = os.path.join(basedir, 'plugin_searchpath')

    # Put extra directory on the python path
    sys.path.append(extra_path)

    yield extra_path

    # Return python path back to normal
    sys.path.remove(extra_path)


def test_discover(extra_pythonpath):
    registry = discovery.autodiscover()

    # possible other plugins in environment
    assert set(registry.keys()) >= set(['foo'])
    assert registry['foo'].open() == 'open_worked'


def test_discover_pluginprefix(extra_pythonpath):
    registry = discovery.autodiscover(plugin_prefix='not_intake_')

    assert set(registry.keys()) == set(['otherfoo'])
    assert registry['otherfoo'].open() == 'open_worked'


def test_discover_collision(extra_pythonpath):
    with pytest.warns(UserWarning):
        registry = discovery.autodiscover(plugin_prefix='collision_')

    assert set(registry.keys()) == set(['foo'])
    assert registry['foo'].open() == 'open_worked'
