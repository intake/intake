#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest


def panel_importable():
    try:
        import panel as pn
        return True
    except:
        return False


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_no_panel_does_not_raise_errors(cat1_url):
    import intake
    cat = intake.open_catalog(cat1_url)
    assert cat.name == 'catalog1'


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_no_panel_display_init_gui():
    import intake
    with pytest.raises(RuntimeError, match=('Please install panel to use the GUI '
                                            '`conda install -c conda-forge panel')):
        repr(intake.gui)


def test_display_init_gui():
    pytest.importorskip('panel')
    import intake
    assert repr(intake.gui).startswith('Column')
