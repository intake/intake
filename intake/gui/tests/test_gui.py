#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import pytest
here = os.path.abspath(os.path.dirname(__file__))


def test_add_cat():
    pytest.importorskip('ipywidgets')
    import intake
    intake.gui.add_cat(os.path.join(here, '..', '..', 'catalog', 'tests',
                                    'catalog1.yml'))
    assert 'catalog1' in intake.gui.cat_list.value
    assert 'entry1' in intake.gui.item_list.options
