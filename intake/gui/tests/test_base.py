#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import pytest

pn = pytest.importorskip('panel')

from ..base import Base


class BaseClass(Base):
    def __init__(self, **kwargs):
        self.panel = pn.Row()
        super().__init__(**kwargs)

    def setup(self):
        self.children = ['fake content']


def test_base_with_panel_gets_populated_when_visible_is_set_to_true():
    base = BaseClass(visible=True)
    assert base.children == ['fake content']
    assert len(base.panel.objects) == 1

    base.visible = False
    assert len(base.panel.objects) == 0
    assert base.children == ['fake content']


def test_base_with_panel_gets_populated_when_visible_is_changed_to_true():
    base = BaseClass(visible=False)
    assert len(base.panel.objects) == 0

    base.visible = True
    assert base.children == ['fake content']
    assert len(base.panel.objects) == 1

