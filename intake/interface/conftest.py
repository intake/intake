# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import shutil

import pytest

import intake

here = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def cat1_url():
    return os.path.join(here, "tests", "catalogs", "catalog1.yaml")


@pytest.fixture
def cat2_url():
    return os.path.join(here, "tests", "catalogs", "catalog2.yaml")


@pytest.fixture
def parent_cat_url():
    return os.path.join(here, "tests", "catalogs", "parent.yaml")


@pytest.fixture
def cat1(cat1_url):
    return intake.open_catalog(cat1_url)


@pytest.fixture
def cat2(cat2_url):
    return intake.open_catalog(cat2_url)


@pytest.fixture
def copycat2(cat2_url, tmp_path):
    parent_dir = os.path.dirname(cat2_url)
    shutil.copytree(parent_dir, tmp_path / "catalogs")
    shutil.copytree(os.path.join(os.path.dirname(parent_dir), "data"), tmp_path / "data")
    return intake.open_catalog(tmp_path / "catalogs" / "catalog2.yaml")


@pytest.fixture
def parent_cat(parent_cat_url):
    return intake.open_catalog(parent_cat_url)


@pytest.fixture
def cat_browser(cat1):
    from .catalog.select import CatSelector

    return CatSelector(cats=[cat1])


@pytest.fixture
def sources1(cat1):
    return list(cat1._entries.values())


@pytest.fixture
def sources2(cat2):
    return list(cat2._entries.values())


@pytest.fixture
def source_browser(sources1):
    from .source.select import SourceSelector

    return SourceSelector(sources=sources1)
