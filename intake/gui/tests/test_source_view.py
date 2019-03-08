#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pn = pytest.importorskip('panel')


def assert_panel_matches_contents(desc):
    """Helper function to check that panel and contents match"""
    assert desc.pane.object == desc.contents
    assert desc.panel.objects == [desc.pane]


def test_description(description):
    assert description.visible
    assert len(description.children) == 1
    assert len(description.contents) > 0
    assert_panel_matches_contents(description)


def test_description_set_source(description, sources1, sources2):
    assert description.source == sources1[0]
    description.source = sources2[0]
    assert description.source == sources2[0]
    assert_panel_matches_contents(description)


def test_description_set_source_from_list(sources2):
    from ..source_view import Description
    description = Description()
    description.source = sources2
    assert description.source == sources2[0]
    assert_panel_matches_contents(description)


def test_description_set_source_from_empty_list(description):
    description.source = []
    assert description.source == None
    assert description.contents == ''
    assert_panel_matches_contents(description)


def test_description_clears_if_visible_is_set_to_false(description):
    description.visible = False
    assert len(description.panel.objects) == 0


def test_description_with_fake_driver_shows_missing_plugin_warning():
    from ..source_view import Description
    from intake.catalog.local import LocalCatalogEntry

    e = LocalCatalogEntry(name='', description='', driver='foo',
                          direct_access='forbid', cache=[],
                          parameters=[], metadata={},
                          catalog_dir='fake/path', args={'arg1': 1})
    description = Description(source=e)
    assert description.contents == ('container: None\n'
                                    'description: \n'
                                    'direct_access: forbid\n'
                                    'user_parameters: []\n'
                                    'Need additional plugin to use foo driver')
    assert_panel_matches_contents(description)

