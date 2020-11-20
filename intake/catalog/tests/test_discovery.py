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
    import intake.catalog
    intake.catalog.builtin = None
    mods = sys.modules.copy()
    try:
        sys.modules.pop("intake")
        sys.modules.pop("intake.catalog")
        intake.catalog.__dict__.pop('builtin')
        assert 'builtin' not in intake.catalog.__dict__
        assert intake.cat is not None
    finally:
        sys.modules.update(mods)
