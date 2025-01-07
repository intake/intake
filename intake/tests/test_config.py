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


@pytest.mark.parametrize(
    "conf",
    [
        {"environment_conf_parse": "error"},
        {"environment_conf_parse": "warn"},
        {"environment_conf_parse": "ignore"},
    ],
)
def test_load_env(conf):
    # test the parsing of environment variables as strings
    os.environ["INTAKE_CACHE"] = "./tmp"  # this causes a SyntaxError
    os.environ["INTAKE_CACHE2"] = "tmp"  # this causes a ValueError
    with temp_conf(conf) as fn:
        if conf["environment_conf_parse"] == "error":
            # When raise_on_error is True, ensure the exception is raised
            with pytest.raises(ValueError, SyntaxError):
                Config(fn)
        elif conf["environment_conf_parse"] == "warn":
            # When raise_on_error is False, ensure the variable is parsed as a string
            with pytest.warns(UserWarning, match="environment variable"):
                config = Config(fn)
            assert config["cache"] == "./tmp"
            assert config["cache2"] == "tmp"
        else:
            with pytest.warns(None) as warnings:
                config = Config(fn)
            assert not warnings
            assert config["cache"] == "./tmp"
            assert config["cache2"] == "tmp"
