import tempfile
import shutil
import os
import pytest
from intake.catalog.zarr import ZarrGroupCatalog
from intake.catalog.local import LocalCatalogEntry
from intake import open_catalog
from intake.source.zarr import ZarrArraySource
from .util import assert_items_equal

zarr = pytest.importorskip('zarr')


@pytest.fixture
def temp_zarr():
    zarr_path = tempfile.mkdtemp()

    # setup a zarr hierarchy stored on local file system
    store = zarr.DirectoryStore(zarr_path)
    root = zarr.open_group(store=store, mode='w')
    root.attrs['title'] = 'root group'
    root.attrs['description'] = 'a test hierarchy'
    root.create_group('foo')
    root['foo'].attrs['title'] = 'foo group'
    root['foo'].attrs['description'] = 'a test group'
    root.create_group('bar')
    root.create_dataset('baz', shape=100, dtype='i4')
    root['baz'].attrs['title'] = 'baz array'
    root['baz'].attrs['description'] = 'a test array'
    root['bar'].create_group('spam')
    root['bar'].create_dataset('eggs', shape=10, dtype='f4')
    zarr.consolidate_metadata(store=store)

    # setup a local YAML file catalog including entries pointing to the zarr
    # hierarchy
    yaml_path = tempfile.mkdtemp()
    catalog_file = os.path.join(yaml_path, 'catalog.yml')
    with open(catalog_file, 'w') as f:
        f.write("""
sources:
  root:
    driver: zarr_cat
    args:
      urlpath: {zarr_path}
      consolidated: True
  bar:
    driver: zarr_cat
    args:
      urlpath: {zarr_path}
      component: bar
  eggs:
    driver: ndzarr
    args:
      urlpath: {zarr_path}
      component: bar/eggs
        """.format(zarr_path=zarr_path))

    yield zarr_path, store, root, catalog_file

    shutil.rmtree(zarr_path)
    shutil.rmtree(yaml_path)


@pytest.mark.parametrize('consolidated', [False, True])
def test_zarr_catalog(temp_zarr, consolidated):
    import dask.array as da

    path, store, root, _ = temp_zarr

    # test zarr catalog opened directly, with different urlpath argument types
    for urlpath in path, store, root:

        # open catalog
        cat = ZarrGroupCatalog(urlpath=urlpath, consolidated=consolidated)
        assert isinstance(cat, ZarrGroupCatalog)
        assert 'catalog' == cat.container

        # check entries
        assert_items_equal(['foo', 'bar', 'baz'], list(cat))
        assert isinstance(cat['foo'], ZarrGroupCatalog)
        assert 'catalog' == cat['foo'].describe()['container']
        assert isinstance(cat['bar'], ZarrGroupCatalog)
        assert 'catalog' == cat['bar'].describe()['container']
        assert isinstance(cat['baz'], ZarrArraySource)
        assert 'ndarray' == cat['baz'].describe()['container']

        # check metadata from attributes
        assert 'root group' == cat.metadata['title']
        assert 'a test hierarchy' == cat.metadata['description']
        assert 'foo group' == cat['foo'].metadata['title']
        assert 'a test group' == cat['foo'].metadata['description']
        # no attributes
        assert 'title' not in cat['bar'].metadata
        assert 'description' not in cat['bar'].metadata

        # check nested catalogs
        assert_items_equal(['spam', 'eggs'], list(cat['bar']))
        assert isinstance(cat['bar']['spam'], ZarrGroupCatalog)
        assert 'catalog' == cat['bar']['spam'].describe()['container']
        assert isinstance(cat['bar']['eggs'], ZarrArraySource)
        assert 'ndarray' == cat['bar']['eggs'].describe()['container']

        # check obtain zarr groups
        assert isinstance(cat.to_zarr(), zarr.hierarchy.Group)
        assert isinstance(cat['foo'].to_zarr(), zarr.hierarchy.Group)
        assert isinstance(cat['bar'].to_zarr(), zarr.hierarchy.Group)
        assert isinstance(cat['bar']['spam'].to_zarr(),
                          zarr.hierarchy.Group)

        # check obtain dask arrays
        assert isinstance(cat['baz'].to_dask(), da.Array)
        assert isinstance(cat['bar']['eggs'].to_dask(), da.Array)

        # open catalog directly from subgroup via `component` arg
        cat = ZarrGroupCatalog(urlpath, component='bar')
        assert_items_equal(['spam', 'eggs'], list(cat))
        assert isinstance(cat['spam'], ZarrGroupCatalog)
        assert 'catalog' == cat['spam'].describe()['container']
        assert isinstance(cat['eggs'], ZarrArraySource)
        assert 'ndarray' == cat['eggs'].describe()['container']


def test_zarr_entries_in_yaml_catalog(temp_zarr):
    import dask.array as da

    # open YAML catalog file
    _, _, _, catalog_file = temp_zarr
    cat = open_catalog(catalog_file)

    # test entries
    assert_items_equal(['root', 'bar', 'eggs'], list(cat))

    # entry pointing to zarr root group
    assert isinstance(cat['root'], ZarrGroupCatalog)
    assert_items_equal(['foo', 'bar', 'baz'], list(cat['root']))
    assert 'catalog' == cat['root'].describe()['container']
    assert isinstance(cat['root'].to_zarr(), zarr.hierarchy.Group)

    # entry pointing to zarr sub-group
    assert isinstance(cat['bar'], ZarrGroupCatalog)
    assert_items_equal(['spam', 'eggs'], list(cat['bar']))
    assert 'catalog' == cat['bar'].describe()['container']
    assert isinstance(cat['bar'].to_zarr(), zarr.hierarchy.Group)

    # entry pointing to zarr array
    assert isinstance(cat['eggs'], ZarrArraySource)
    assert 'ndarray' == cat['eggs'].describe()['container']
    assert isinstance(cat['eggs'].to_dask(), da.Array)
