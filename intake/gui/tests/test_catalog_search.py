#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pn = pytest.importorskip('panel')


@pytest.fixture
def search(cat1, cat2):
    from ..catalog_search import Search
    return Search(cats=[cat1, cat2])


def test_search(search):
    assert search.visible
    assert len(search.children) == 5
    assert len(search.panel.objects) == 5


def test_search_watchers_gets_populated(search):
    assert len(search.watchers) == 1


def test_search_widget_click_tries_to_run_callback(search):
    search.text.value = 'entry'
    with pytest.raises(TypeError, match="'NoneType' object is not callable"):
        search.widget.clicks = 1


def test_search_unwatch_watchers_get_cleaned_up(search):
    search.unwatch()
    assert len(search.watchers) == 0
    search.text.value = 'entry'

    # does not try to run callback
    search.widget.clicks = 2


def test_callback_gets_right_input(search):
    def callback(new_cats):
        """Raises an error if called"""
        raise ValueError('New catalogs', new_cats)

    search.text.value = 'entry'
    search.done_callback = callback
    with pytest.raises(ValueError, match='<Intake catalog: catalog1_search>'):
        search.widget.clicks = 3
