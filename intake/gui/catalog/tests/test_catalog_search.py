#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pytest.importorskip('panel')


@pytest.fixture
def search_inputs(cat1, cat2):
    from ..search import SearchInputs
    return SearchInputs()


def test_search_inputs(search_inputs):
    assert search_inputs.visible
    assert len(search_inputs.children) == 4
    assert len(search_inputs.panel.objects) == 4


def test_search_inputs_text_prop_equal_to_widget_value(search_inputs):
    search_inputs.text = 'some text'
    assert search_inputs.text_widget.value == 'some text'


def test_search_inputs_depth_prop_parses_to_int(search_inputs):
    search_inputs.depth = '2'
    assert search_inputs.depth == 2

    search_inputs.depth = 'All'
    assert search_inputs.depth == 99


@pytest.fixture
def search(cat1, cat2):
    from ..search import Search
    return Search(cats=[cat1, cat2])


def test_search(search):
    assert search.visible
    assert len(search.children) == 2
    assert len(search.panel.objects) == 2


def test_search_watchers_gets_populated(search):
    assert len(search.watchers) == 1


def test_search_widget_click_tries_to_run_callback(search):
    search.inputs.text = 'flight'
    with pytest.raises(TypeError, match="'NoneType' object is not callable"):
        search.widget.clicks = 1


def test_search_unwatch_watchers_get_cleaned_up(search):
    search.unwatch()
    assert len(search.watchers) == 0
    search.inputs.text = 'flight'

    # does not try to run callback
    search.widget.clicks = 2


def test_callback_gets_right_input(search):
    def callback(new_cats):
        """Raises an error if called"""
        raise ValueError('New catalogs', new_cats)

    search.inputs.text = 'flight'
    search.done_callback = callback
    with pytest.raises(ValueError, match='<Intake catalog: catalog1_search>'):
        search.widget.clicks = 3
