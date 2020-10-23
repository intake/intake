import copy
import glob
import os
import pytest
import sys

from ..local import YAMLFilesCatalog, MergedCatalog, EntrypointsCatalog


def test_catalog_discovery():
    basedir = os.path.dirname(__file__)
    yaml_glob = os.path.join(basedir, 'catalog_search', '*.yml')
    example_packages = os.path.join(basedir, 'catalog_search', 'example_packages')

    test_catalog = MergedCatalog([EntrypointsCatalog(paths=[example_packages]),
                                  YAMLFilesCatalog(path=[yaml_glob])])

    assert 'use_example1' in test_catalog
    assert 'ep1' in test_catalog


def test_deferred_import():
    "See https://github.com/intake/intake/pull/541"
    # We are going to mess with sys.modules here, so to be safe let's put it
    # back the way it was at the end.
    modules = sys.modules.copy()
    del sys.modules["intake"]
    try:
        import intake
        assert intake.catalog is None
        assert intake.cat is not None
        assert intake.catalog is not None  # implementation detail
    finally:
        # Put sys.modules back as it was before we messed with it.
        sys.modules = modules
