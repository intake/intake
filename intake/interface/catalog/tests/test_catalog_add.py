#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import os
import pytest
pn = pytest.importorskip('panel')

def callback(args):
    """Raises an error if called"""
    raise ValueError('Callback provided:', args)


@pytest.fixture
def file_selector():
    from ..add import FileSelector
    return FileSelector()


@pytest.fixture
def url_selector():
    from ..add import URLSelector
    return URLSelector()


@pytest.fixture
def cat_adder():
    from ..add import CatAdder
    return CatAdder()


def test_file_selector(file_selector):
    assert file_selector.path == os.getcwd() + '/'


def test_file_selector_raises_error_if_no_file_selected(file_selector):
    with pytest.raises(IndexError, match='list index out of range'):
        file_selector.url


def test_file_selector_edit_path(file_selector):
    expected = os.getcwd()
    file_selector.move_up()
    file_selector.path_text.value = os.getcwd()
    assert file_selector.validator.object is None
    assert file_selector.path == expected


def test_file_selector_go_home(file_selector):
    expected = os.getcwd() + '/'
    assert file_selector.path == expected
    file_selector.move_up()
    file_selector.go_home()
    assert file_selector.path == expected


def test_file_selector_move_up(file_selector):
    assert file_selector.path == os.getcwd() + '/'
    file_selector.move_up()
    expected = os.path.abspath('..')
    assert file_selector.path == expected


def test_file_selector_move_down(file_selector):
    expected = os.getcwd() + '/'
    dirname = expected.split('/')[-2] + '/'

    # move up so that we know we will be able to move down into
    # intial dir
    file_selector.move_up()

    # setting the value on main widget will trigger move down
    file_selector.main.value = [dirname]
    assert file_selector.path == expected

    # should empty the selection on main
    assert file_selector.main.value == []

def test_url_selector(url_selector):
    assert url_selector.url == ''
    assert url_selector.visible
    assert len(url_selector.panel.objects) == 2

def test_url_selector_set_visible_to_false(url_selector):
    url_selector.visible = False
    assert url_selector.visible is False
    assert len(url_selector.panel.objects) == 0


def test_cat_adder(cat_adder):
    assert cat_adder.visible is True
    assert cat_adder.tabs.active == 0
    assert cat_adder.widget.disabled is True
    assert len(cat_adder.panel.objects) == 2

    cat_adder.tabs.active = 1
    assert cat_adder.cat_url[0] == ''
    assert cat_adder.cat.name is None
    assert cat_adder.widget.disabled is False

    cat_adder.done_callback = callback
    with pytest.raises(ValueError, match='None'):
        cat_adder.add_cat()

def test_cat_adder_add_real_cat(cat_adder, cat1_url, cat1):
    cat_adder.tabs.active = 1
    cat_adder.url.main.value = cat1_url

    assert cat_adder.cat_url[0] == cat1_url
    assert cat_adder.cat == cat1

    cat_adder.done_callback = callback
    with pytest.raises(ValueError, match=str(cat1)):
        cat_adder.add_cat()
