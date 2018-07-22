import os
import shutil
import time

import intake
import appdirs
import pytest


def copy_test_file(filename, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)  # can't use exist_ok in Python 2.7

    # Put a catalog file in the user catalog directory
    test_dir = os.path.dirname(__file__)
    test_catalog = os.path.join(test_dir, filename)
    target_catalog = os.path.join(target_dir, '__unit_test_'+filename)

    shutil.copyfile(test_catalog, target_catalog)
    return target_catalog    


@pytest.fixture
def user_catalog():
    target_catalog = copy_test_file('catalog1.yml',
                                    appdirs.user_data_dir(appname='intake',
                                                          appauthor='intake'))
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
    time.sleep(2) # wait 2 seconds for catalog to refresh
    assert set(cat) >= set(['ex1', 'ex2'])


def test_bad_open():
    with pytest.raises(ValueError):
        intake.open_catalog(None, driver='unknown')


def test_output_notebook():
    pytest.importorskip('hvplot')
    intake.output_notebook()