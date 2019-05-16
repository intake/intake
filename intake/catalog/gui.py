#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from functools import partial

try:
    import panel as pn
    from ..gui.gui import SourceGUI, MAX_WIDTH
    css = """
    .scrolling {
        overflow: scroll;
    }
    """
    pn.config.raw_css.append(css)  # add scrolling class from css (panel GH#383, GH#384)
    pn.extension()

    class EntryGUI(SourceGUI):
        def __init__(self, source=None, **kwargs):
            self.source = source
            # set logo to false because we'll use a special logo_panel
            kwargs['logo'] = False
            super().__init__(sources=[self.source], **kwargs)
            self.panel = pn.Row(name='Source')

        def setup(self):
            super().setup()
            self.select.visible = False
            self.children = [
                pn.Column(
                    self.logo_panel,
                    *self.controls,
                    margin=0,
                ),
                pn.Column(
                    self.description.panel,
                    self.plot.panel,
                    width_policy='max',
                    max_width=MAX_WIDTH,
                ),
            ]

        @property
        def item(self):
            return self.source

    class CatalogGUI(SourceGUI):
        def __init__(self, cat, logo=True, **kwargs):
            self.cat = cat
            super().__init__(cats=[self.cat], logo=logo, **kwargs)

        @property
        def item(self):
            """Item that is selected"""
            if len(self.sources) == 0:
                return None
            return self.sources[0]

except ImportError:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass
        def __repr__(self):
            raise RuntimeError("Please install panel to use the GUI (`conda install -c conda-forge panel==0.5.1`)")

    EntryGUI = GUI
    CatalogGUI = GUI

except Exception as e:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass
        def __repr__(self):
            raise RuntimeError("Initialization of GUI failed, even though "
                               "panel is installed. Please update it "
                               "to a more recent version (`conda install -c conda-forge panel==0.5.1`).")
    EntryGUI = GUI
    CatalogGUI = GUI
