#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
pn = pytest.importorskip('panel')


@pytest.fixture
def description(sources1):
    from ..source_view import Description
    return Description(source=sources1[0])


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
    assert description.contents == ' ' * 100
    assert_panel_matches_contents(description)


def test_description_clears_if_visible_is_set_to_false(description):
    description.visible = False
    assert len(description.panel.objects) == 0


def test_description_with_fake_driver_shows_missing_plugin_warning(sources1):
    from ..source_view import Description

    description = Description(source=sources1[1])
    assert description.contents == ('container: None\n'
                                    'description: \n'
                                    'direct_access: forbid\n'
                                    'user_parameters: []\n'
                                    'Need additional plugin to use fake driver')
    assert_panel_matches_contents(description)


def test_description_source_with_plots(sources2):
    from ..source_view import Description
    description = Description(source=sources2[0])
    assert description.source == sources2[0]
    assert description.contents == (
        'container: dataframe\n'
        'description: US Crime data [UCRDataTool](https://www.ucrdatatool.gov/Search/Crime/State/StatebyState.cfm)\n'
        'direct_access: forbid\n'
        'user_parameters: []\n'
        'plugin: csv\n'
        'metadata: cache: []\n'
        'args: urlpath: /Users/jsignell/intake/intake/gui/tests/catalogs/../data/crime.csv\n'
        '  metadata: cache: []\n'
        '    catalog_dir: /Users/jsignell/intake/intake/gui/tests/catalogs/')
    assert_panel_matches_contents(description)


def assert_is_empty(plots, visible=True):
    assert plots.source is None
    assert plots.has_plots is False
    assert plots.instructions_contents == '*No predefined plots found - declare these in the catalog*'
    assert plots.options == []
    assert plots.selected is None
    if visible:
        assert plots.instructions.object == plots.instructions_contents
        assert plots.desc.object is None
        assert plots.pane.object is None
        assert len(plots.children) == 2
        assert isinstance(plots.children[1], pn.pane.HoloViews)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 1
    else:
        assert plots.watchers == []
        assert plots.panel.objects == []


def assert_plotting_source2_0_line(plots, visible=True):
    assert plots.has_plots is True
    assert plots.instructions_contents == '**Select from the predefined plots:**'
    assert plots.options == ['line_example', 'violin_example']
    if visible:
        assert plots.selected == 'line_example'
        assert plots.instructions.object == plots.instructions_contents
        assert plots.desc.object == ("kind: line\n"
                                     "y: ['Robbery', 'Burglary']\n"
                                     "x: Year")
        assert plots.pane.object is not None
        assert len(plots.children) == 2
        assert isinstance(plots.children[1], pn.pane.HoloViews)
        assert str(plots.children[1].object) == str(plots.pane.object)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 1
    else:
        assert plots.selected is None
        assert plots.watchers == []
        assert plots.panel.objects == []


@pytest.fixture
def defined_plots(sources2):
    from ..source_view import DefinedPlots
    return DefinedPlots(source=sources2[0])


def test_defined_plots(defined_plots, sources2):
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=True)


def test_defined_plots_init_empty_and_not_visible_set_source(sources2):
    from ..source_view import DefinedPlots
    defined_plots = DefinedPlots(source=[], visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=False)


def test_defined_plots_init_with_source_not_visible_make_visible(sources2):
    from ..source_view import DefinedPlots
    defined_plots = DefinedPlots(source=sources2, visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=False)

    defined_plots.visible = True
    assert_plotting_source2_0_line(defined_plots, visible=True)


def test_defined_plots_init_empty_and_visible():
    from ..source_view import DefinedPlots
    defined_plots = DefinedPlots()
    assert_is_empty(defined_plots, visible=True)


def test_defined_plots_init_empty_and_not_visible():
    from ..source_view import DefinedPlots
    defined_plots = DefinedPlots(visible=False)
    assert_is_empty(defined_plots, visible=False)


def test_defined_plots_set_source_to_empty_list(defined_plots):
    defined_plots.source = []
    assert_is_empty(defined_plots, visible=True)


def test_defined_plots_set_source_to_empty_list_and_visible_to_false(defined_plots):
    defined_plots.visible = False
    defined_plots.source = []
    assert_is_empty(defined_plots, visible=False)


def test_defined_plots_select_a_different_plot(defined_plots):
    """ This one is a genuine failure """
    defined_plots.selected = 'violin_example'
    assert defined_plots.desc.object.startswith("kind: violin")
    assert len(defined_plots.children) == 2
    assert isinstance(defined_plots.children[1], pn.pane.HoloViews)
    assert str(defined_plots.children[1].object) == str(defined_plots.pane.object)
    assert defined_plots.panel.objects == defined_plots.children
