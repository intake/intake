import copy
import glob
import os
import pytest

from ..local import YAMLFilesCatalog, MergedCatalog, EntrypointsCatalog


def test_catalog_discovery():
    basedir = os.path.dirname(__file__)
    path = os.path.join(basedir, 'catalog_search')
    collision_path = os.path.join(path, 'yaml')

    test_catalog = MergedCatalog([EntrypointsCatalog(paths=[path]),
                                  YAMLFilesCatalog(path=[path])])

    assert 'yaml' in test_catalog
    assert 'ep1' in test_catalog

    with pytest.warns(UserWarning):
        test_catalog = MergedCatalog([EntrypointsCatalog(paths=[path]),
                                      YAMLFilesCatalog(path=[path, collision_path])])

    assert 'yaml' in test_catalog
    assert 'ep1' in test_catalog
