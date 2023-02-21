# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

try:
    import panel as pn

    from ..interface.gui import ICONS, SourceGUI

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
            super().__init__(sources=[self.source], **kwargs)
            self.panel = pn.Column(pn.Row(ICONS["logo"], pn.Column(*self.controls)), self.description.panel, self.plot.panel)

        def setup(self):
            self._setup_watchers()

        @SourceGUI.visible.setter
        def visible(self, visible):
            self._visible = visible

            if visible:
                self.setup()
                self.select.visible = True
                self.description.visible = True
            elif not visible:
                self.unwatch()
                # do children
                self.select.visible = False
                self.description.visible = False
                self.plot.visible = False
            if self.visible_callback:
                self.visible_callback(visible)

        @property
        def item(self):
            return self.source

    class CatalogGUI(SourceGUI):
        def __init__(self, cat, **kwargs):
            self.cat = cat
            super().__init__(cats=[self.cat], **kwargs)
            self.panel = pn.Column(
                pn.Row(
                    pn.panel(ICONS["logo"]),
                    pn.Row(pn.Column(self.select.panel, self.control_panel, margin=0), self.description.panel, margin=0),
                ),
                self.plot.panel,
            )

        def setup(self):
            self._setup_watchers()

        @SourceGUI.visible.setter
        def visible(self, visible):
            self._visible = visible

            if visible:
                self.setup()
                self.select.visible = True
                self.description.visible = True
                if len(self.control_panel.objects) == 0:
                    self.control_panel.extend(self.controls)
            elif not visible:
                self.unwatch()
                # do children
                self.select.visible = False
                self.control_panel.clear()
                self.description.visible = False
                self.plot.visible = False
            if self.visible_callback:
                self.visible_callback(visible)

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
            raise RuntimeError("Please install panel to use the GUI (`conda " "install -c conda-forge panel>0.8.0`)")

    EntryGUI = GUI
    CatalogGUI = GUI

except Exception:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass

        def __repr__(self):
            raise RuntimeError(
                "Initialization of GUI failed, even though "
                "panel is installed. Please update it "
                "to a more recent version (`conda install -c "
                "conda-forge panel==0.5.1`)."
            )

    EntryGUI = GUI
    CatalogGUI = GUI
