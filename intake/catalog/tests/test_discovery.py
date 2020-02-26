import copy
import glob
import os
import pytest

from ..local import YAMLFilesCatalog, MergedCatalog, EntrypointsCatalog


def test_catalog_discovery():
    basedir = os.path.dirname(__file__)
    yaml_glob = os.path.join(basedir, 'catalog_search', '*.yml')
    example_packages = os.path.join(basedir, 'catalog_search', 'example_packages')

    test_catalog = MergedCatalog([EntrypointsCatalog(paths=[example_packages]),
                                  YAMLFilesCatalog(path=[yaml_glob])])

    assert 'use_example1' in test_catalog
    assert 'ep1' in test_catalog
