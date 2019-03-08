#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import pytest

pn = pytest.importorskip('panel')


def test_base():
    from ..base import Base
    base = Base()
    assert base.visible == True


def test_base_with_panel_clears_when_not_visible():
    from ..base import Base
    base = Base()
    base.children = ['fake content']
    base.panel = pn.Column(*base.children)
    assert base.visible == True
    base.visible = False
    assert len(base.panel.objects) == 0
    assert base.children == ['fake content']


def test_base_with_panel_gets_populated_when_visible_is_set_to_true():
    from ..base import Base

    class BaseClass(Base):
        def __init__(self, visible):
            self.panel = pn.Row()
            self.visible = visible

        def setup(self):
            self.children = ['fake content']

    base = BaseClass(visible=True)
    assert base.children == ['fake content']
    assert len(base.panel.objects) == 1


def test_base_with_panel_gets_populated_when_visible_is_changed_to_true():
    from ..base import Base

    class BaseClass(Base):
        def __init__(self, visible):
            self.panel = pn.Row()
            self.visible = visible

        def setup(self):
            self.children = ['fake content']

    base = BaseClass(visible=False)
    assert len(base.panel.objects) == 0

    base.visible = True
    assert base.children == ['fake content']
    assert len(base.panel.objects) == 1

