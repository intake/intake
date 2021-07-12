#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import posixpath
import sys
import tempfile
import time

import appdirs
import intake
import intake.catalog.local
import pytest

from .test_utils import copy_test_file


@pytest.fixture
def user_catalog():
    target_catalog = copy_test_file('catalog1.yml',
                                    appdirs.user_data_dir(appname='intake',
                                                          appauthor='intake'))
    yield target_catalog
    # Remove the file, but not the directory (because there might be other
    # files already there)
    os.remove(target_catalog)


@pytest.fixture
def tmp_path_catalog():
    tmp_path = posixpath.join(tempfile.gettempdir(), 'intake')
    try:
        os.makedirs(tmp_path)
    except:
        pass
    target_catalog = copy_test_file('catalog1.yml', tmp_path)
    yield target_catalog
    # Remove the file, but not the directory (because there might be other
    # files already there)
    os.remove(target_catalog)


def test_autoregister_open():
    assert hasattr(intake, 'open_csv')


def test_default_catalogs():
    # No assumptions about contents of these catalogs.
    # Just make sure they exist and don't raise exceptions
    list(intake.cat)


def test_user_catalog(user_catalog):
    cat = intake.load_combo_catalog()
    assert set(cat) >= set(['ex1', 'ex2'])


def test_open_styles(tmp_path_catalog):
    cat = intake.catalog.local.YAMLFileCatalog(tmp_path_catalog)
    cat2 = intake.open_catalog(tmp_path_catalog)
    assert list(cat) == list(cat2)
    cat2 = intake.open_catalog([tmp_path_catalog])
    assert list(cat) == list(cat2)
    cat2 = intake.open_catalog(os.path.join(
        os.path.dirname(tmp_path_catalog), "*"))
    assert list(cat) == list(cat2)
    assert type(cat2).name == 'yaml_files_cat'
    cat2 = intake.open_catalog(os.path.dirname(tmp_path_catalog))
    assert list(cat) == list(cat2)
    assert type(cat2).name == 'yaml_files_cat'
    cat2 = intake.open_yaml_file_cat(tmp_path_catalog)
    assert list(cat) == list(cat2)
    cat2 = intake.open_yaml_files_cat([tmp_path_catalog])
    assert list(cat) == list(cat2)
    cat2 = intake.open_yaml_files_cat(os.path.join(
        os.path.dirname(tmp_path_catalog), "*"))
    assert list(cat) == list(cat2)


def test_path_catalog(tmp_path_catalog):
    intake.config.conf['catalog_path'] = [posixpath.join(tempfile.gettempdir(), 'intake')]
    cat = intake.load_combo_catalog()
    time.sleep(2) # wait 2 seconds for catalog to refresh
    assert set(cat) >= set(['ex1', 'ex2'])
    del intake.config.conf['catalog_path']


def test_bad_open():
    with pytest.raises(ValueError):
        # unknown driver
        intake.open_catalog("", driver='unknown')
    with pytest.raises(ValueError):
        # bad URI type (NB falsish values become empty catalogs)
        intake.open_catalog(True)
    # default empty catalog
    assert intake.open_catalog() == intake.open_catalog(None)


def test_output_notebook():
    pytest.importorskip('hvplot')
    intake.output_notebook()


def test_old_usage():
    assert isinstance(intake.Catalog(), intake.Catalog)
    assert intake.Catalog is intake.catalog.base.Catalog


def test_no_imports():
    mods = [mod for mod in sys.modules if mod.startswith('intake')]
    [sys.modules.pop(mod) for mod in mods]

    import intake

    assert 'intake' in sys.modules

    for mod in ['intake.tests', 'intake.interface', 'intake.source.csv',
                'intake.cli', 'intake.auth']:
        assert mod not in sys.modules


@pytest.fixture
def tmp_path_catalog_nested():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = posixpath.join(tmp_dir, 'intake')
        target_catalog = copy_test_file('catalog_nested.yml', tmp_path)
        copy_test_file('catalog_nested_sub.yml', tmp_path)
        yield target_catalog


def test_nested_catalog_access(tmp_path_catalog_nested):
    cat = intake.open_catalog(tmp_path_catalog_nested)
    entry1 = cat.nested.ex1
    entry2 = cat["nested.ex1"]
    entry3 = cat[["nested", "ex1"]]
    entry4 = cat["nested", "ex1"]
    assert entry1 == entry2 == entry3 == entry4
