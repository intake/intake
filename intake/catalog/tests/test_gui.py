#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import pytest
import os


def panel_importable():
    try:
        import panel as pn
        return True
    except:
        return False


EXPECTED_ERROR_TEXT = "Please install panel to use the GUI"


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_cat_no_panel_does_not_raise_errors(catalog1):
    assert catalog1.name == 'name_in_cat'


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_cat_no_panel_display_gui(catalog1):
    with pytest.raises(RuntimeError, match=EXPECTED_ERROR_TEXT):
        repr(catalog1.gui)


def test_cat_gui(catalog1):
    pytest.importorskip('panel')
    assert repr(catalog1.gui).startswith('Column')


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_entry_no_panel_does_not_raise_errors(catalog1):
    assert catalog1.entry1.name == 'entry1'


@pytest.mark.skipif(panel_importable(), reason="panel is importable, so skip")
def test_entry_no_panel_display_gui(catalog1):
    with pytest.raises(RuntimeError, match=EXPECTED_ERROR_TEXT):
        repr(catalog1.entry1.gui)


def test_entry_gui(catalog1):
    pytest.importorskip('panel')
    assert repr(catalog1.entry1.gui).startswith('Column')
