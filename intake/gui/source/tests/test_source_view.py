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
    from ..description import Description
    return Description(source=sources1[0])


def assert_panel_matches_contents(desc):
    """Helper function to check that panel and contents match"""
    assert desc.label_pane.object == desc.label
    assert desc.main_pane.object == desc.contents
    assert desc.panel.objects == [desc.label_pane, desc.main_pane]


def test_description(description):
    assert description.visible
    assert len(description.children) == 2
    assert len(description.contents) > 0
    assert_panel_matches_contents(description)


def test_description_set_source(description, sources1, sources2):
    assert description.source == sources1[0]
    description.source = sources2[0]
    assert description.source == sources2[0]
    assert_panel_matches_contents(description)


def test_description_set_source_from_list(sources2):
    from ..description import Description
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


def test_description_source_with_plots(sources2):
    from ..description import Description
    description = Description(source=sources2[0])
    assert description.source == sources2[0]
    lines = (
        'name: us_crime\n'
        'container: dataframe\n'
        "plugin: ['csv']\n"
        'description: US Crime data [UCRDataTool](https://www.ucrdatatool.gov'
        '/Search/Crime/State/StatebyState.cfm)\n'
        'direct_access: forbid\n'
        'user_parameters: []\n'
        'metadata: \n'
        'args:\nurlpath: {{ CATALOG_DIR }}../data/crime.csv').split('\n')
    for line in lines:
        assert line in description.contents
    assert 'plots' in description.contents
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
        assert len(plots.children) == 3
        assert isinstance(plots.children[-1], pn.pane.HoloViews)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 2
    else:
        assert not plots.selected
        assert not plots.watchers
        assert not plots.panel.objects


def assert_plotting_source2_0_line(plots, visible=True, desc=False):
    assert plots.has_plots is True
    assert plots.instructions_contents == '**Select from the predefined plots:**'
    assert plots.options == ['line_example', 'violin_example']
    if visible:
        assert plots.selected == 'line_example'
        assert plots.instructions.object == plots.instructions_contents
        if desc:
            assert plots.desc.object == ("kind: line\n"
                                         "y: ['Robbery', 'Burglary']\n"
                                         "x: Year")
        else:
            assert plots.desc.object == None
        assert plots.pane.object is not None
        assert len(plots.children) == 3
        assert isinstance(plots.children[-1], pn.pane.HoloViews)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 2
    else:
        assert not plots.selected
        assert not plots.watchers
        assert not plots.panel.objects


@pytest.fixture
def defined_plots(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import DefinedPlots
    return DefinedPlots(source=sources2[0])


def test_defined_plots_toggle_desc(defined_plots, sources2):
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=True)

    defined_plots.show_desc.value = True
    assert_plotting_source2_0_line(defined_plots, visible=True, desc=True)


def test_defined_plots_init_empty_and_not_visible_set_source(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import DefinedPlots
    defined_plots = DefinedPlots(source=[], visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=False)


def test_defined_plots_init_with_source_not_visible_make_visible(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import DefinedPlots
    defined_plots = DefinedPlots(source=sources2, visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]
    assert_plotting_source2_0_line(defined_plots, visible=False)

    defined_plots.visible = True
    assert_plotting_source2_0_line(defined_plots, visible=True)


def test_defined_plots_init_empty_and_visible():
    from ..defined_plots import DefinedPlots
    defined_plots = DefinedPlots()
    assert_is_empty(defined_plots, visible=True)


def test_defined_plots_init_empty_and_not_visible():
    from ..defined_plots import DefinedPlots
    defined_plots = DefinedPlots(visible=False)
    assert_is_empty(defined_plots, visible=False)


def test_defined_plots_set_source_to_empty_list(defined_plots):
    defined_plots.source = []
    assert_is_empty(defined_plots, visible=True)


def test_defined_plots_set_source_to_empty_list_and_visible_to_false(defined_plots):
    defined_plots.visible = False
    defined_plots.source = []
    assert_is_empty(defined_plots, visible=False)


@pytest.mark.skip(reason='This one is failing - but works in widget')
def test_defined_plots_select_a_different_plot(defined_plots):
    defined_plots.selected = 'violin_example'
    assert defined_plots.desc.object.startswith("kind: violin")
    assert len(defined_plots.children) == 2
    assert isinstance(defined_plots.children[1], pn.pane.HoloViews)
    assert str(defined_plots.children[1].object) == str(defined_plots.pane.object)
    assert defined_plots.panel.objects == defined_plots.children
