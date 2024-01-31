# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os

import pytest

from intake import config
from intake.config import Config, defaults
from intake.util_tests import temp_conf


@pytest.mark.parametrize("conf", [{}, {"port": 5000}, {"other": True}])
def test_load_conf(conf):
    # This test will only work if your config is set to default
    inconf = defaults.copy()
    expected = inconf.copy()
    with temp_conf(conf) as fn:
        config = Config(fn)
        config.load(fn)
        expected.update(conf)
        assert dict(config) == expected
        config.reset()
        assert dict(config) == inconf


@pytest.mark.skipif(os.name == "nt", reason="Paths are different on win")
def test_pathdirs():
    assert config.intake_path_dirs([]) == []
    assert config.intake_path_dirs(["paths"]) == ["paths"]
    assert config.intake_path_dirs("") == [""]
    assert config.intake_path_dirs("path1:path2") == ["path1", "path2"]
    assert config.intake_path_dirs("memory://path1:memory://path2") == [
        "memory://path1",
        "memory://path2",
    ]
