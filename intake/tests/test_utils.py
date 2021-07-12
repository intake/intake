#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import os
import posixpath
import shutil

import pytest
import yaml
from intake.utils import make_path_posix, no_duplicate_yaml


def test_windows_file_path():
    path = 'C:\\Users\\user\\fake.file'
    actual = make_path_posix(path)
    expected = 'C:/Users/user/fake.file'
    assert actual == expected


def test_make_path_posix_removes_double_sep():
    path = 'user//fake.file'
    actual = make_path_posix(path)
    expected = 'user/fake.file'
    assert actual == expected


@pytest.mark.parametrize('path', [
    '~/fake.file',
    'https://example.com',
])
def test_noops(path):
    """For non windows style paths, make_path_posix should be a noop"""
    assert make_path_posix(path) == path


def test_roundtrip_file_path():
    path = os.path.dirname(__file__)
    actual = make_path_posix(path)
    assert '\\' not in actual
    assert os.path.samefile(actual, path)


def test_yaml_tuples():
    data = (1, 2)
    text = yaml.dump(data)
    with no_duplicate_yaml():
        assert yaml.safe_load(text) == data

def copy_test_file(filename, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)  # can't use exist_ok in Python 2.7
    target_dir = make_path_posix(target_dir)
    # Put a catalog file in the user catalog directory
    test_dir = make_path_posix(os.path.dirname(__file__))
    test_catalog = posixpath.join(test_dir, filename)
    target_catalog = posixpath.join(target_dir, '__unit_test_'+filename)

    shutil.copyfile(test_catalog, target_catalog)
    return target_catalog
