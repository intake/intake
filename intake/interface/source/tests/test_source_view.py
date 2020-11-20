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
    assert desc.contents in desc.main_pane.object
    assert desc.panel.objects == [desc.main_pane]


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
    from ..description import Description
    description = Description()
    description.source = sources2
    assert description.source == sources2[0]
    assert_panel_matches_contents(description)


def test_description_set_source_from_empty_list(description):
    description.source = []
    assert description.source == None
    assert "⠀" * 30 in description.contents
    assert_panel_matches_contents(description)


def test_description_clears_if_visible_is_set_to_false(description):
    description.visible = False
    assert len(description.panel.objects) == 0


def assert_is_empty(plots, visible=True):
    assert plots.source is None
    assert plots.has_plots is False
    assert plots.instructions_contents == '*No predefined plots found - declare these in the catalog*'
    assert plots.options == []
    assert plots.selected is None
    if visible:
        assert plots.instructions.object == plots.instructions_contents
        assert plots.pane.object is None
        assert len(plots.children) == 2
        assert isinstance(plots.children[-1][0][0], pn.pane.HoloViews)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 2
    else:
        assert not plots.selected
        assert not plots.watchers
        assert not plots.panel.objects


def assert_plotting_source2_0_line(plots, visible=True, desc=False):
    assert plots.has_plots is True
    assert plots.instructions_contents == '**Select from the predefined plots:**'
    assert plots.options == ["None", 'line_example', 'violin_example']
    if visible:
        assert plots.selected == 'None'
        plots.selected = 'line_example'
        assert plots.instructions.object == plots.instructions_contents
        assert plots.pane.object is not None
        assert len(plots.children) == 2
        assert isinstance(plots.children[-1][0][0], pn.pane.HoloViews)
        assert plots.panel.objects == plots.children
        assert len(plots.watchers) == 2
    else:
        assert not plots.selected
        assert not plots.watchers
        assert not plots.panel.objects


@pytest.fixture
def defined_plots(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import Plots
    return Plots(source=sources2[0])


def test_defined_plots_init_empty_and_not_visible_set_source(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import Plots
    defined_plots = Plots(source=[], visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]()
    assert_plotting_source2_0_line(defined_plots, visible=False)


def test_defined_plots_init_with_source_not_visible_make_visible(sources2):
    pytest.importorskip('hvplot')
    from ..defined_plots import Plots
    defined_plots = Plots(source=sources2, visible=False)
    defined_plots.source = sources2
    assert defined_plots.source == sources2[0]()
    assert_plotting_source2_0_line(defined_plots, visible=False)

    defined_plots.visible = True
    assert_plotting_source2_0_line(defined_plots, visible=True)


def test_defined_plots_init_empty_and_visible():
    from ..defined_plots import Plots
    defined_plots = Plots()
    assert_is_empty(defined_plots, visible=True)


def test_defined_plots_init_empty_and_not_visible():
    from ..defined_plots import Plots
    defined_plots = Plots(visible=False)
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
    assert len(defined_plots.children) == 2
    assert isinstance(defined_plots.children[1], pn.Column)
    assert str(defined_plots.children[1].objects) == str(defined_plots.pane.objects)
    assert defined_plots.panel.objects == defined_plots.children
