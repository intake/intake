#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from functools import partial

try:
    import panel as pn
    from ..gui import *

    class EntryGUI(Base):
        def __init__(self, source=None, **kwargs):
            self.source = source
            self.panel = pn.Column(name='GUI', width_policy='max', max_width=MAX_WIDTH)
            super().__init__(**kwargs)

        def setup(self):
            self.plot_toggle = pn.widgets.Toggle(
                name='📊',
                value=False,
                disabled=len(self.source.plots) == 0,
                width=50)

            self.description = Description(source=self.source)
            self.plot = DefinedPlots(source=self.source,
                                     visible=self.plot_toggle.value,
                                     visible_callback=partial(setattr, self.plot_toggle, 'value'))

            self.watchers = [
                self.plot_toggle.link(self.plot, value='visible'),
            ]

            self.children = [
                pn.Row(
                    pn.Column(
                        logo,
                        self.plot_toggle
                    ),
                    self.description.panel,
                    background=BACKGROUND,
                    width_policy='max',
                    max_width=MAX_WIDTH,
                ),
                self.plot.panel,
            ]

        @property
        def item(self):
            return self.source

    class CatalogGUI(Base):
        def __init__(self, cat, **kwargs):
            self.cat = cat
            self.panel = pn.Column(name='GUI', width_policy='max', max_width=MAX_WIDTH)
            super().__init__(**kwargs)

        def setup(self):
            self.plot_toggle = pn.widgets.Toggle(
                name='📊',
                value=False,
                width=50)

            self.source_browser = SourceSelector(cats=[self.cat],
                                                 enable_dependent=self.enable_plot)
            self.description = Description(source=self.sources)

            self.plot = DefinedPlots(source=self.sources,
                                     visible=self.plot_toggle.value,
                                     visible_callback=partial(setattr, self.plot_toggle, 'value'))

            # set disabled after item is selected
            self.plot_toggle.disabled = self.item is None or len(self.item.plots) == 0

            self.watchers = [
                self.plot_toggle.param.watch(self.on_click_plot, 'value'),
                self.source_browser.widget.link(self.description, value='source'),
            ]

            self.children = [
                pn.Row(
                    pn.Column(
                        logo,
                        self.plot_toggle
                    ),
                    self.source_browser.panel,
                    self.description.panel,
                    background=BACKGROUND,
                    width_policy='max',
                    max_width=MAX_WIDTH,
                ),
                self.plot.panel,
            ]

        def enable_plot(self, enable):
            if not enable:
                self.plot_toggle.value = False
                return enable_widget(self.plot_toggle, enable)

        def on_click_plot(self, event):
            """ When the plot control is toggled, set visibility and hand down source"""
            self.plot.source = self.sources
            self.plot.visible = event.new
            if self.plot.visible:
                self.plot.watchers.append(
                    self.source_browser.widget.link(self.plot, value='source'))

        @property
        def sources(self):
            return self.source_browser.selected

        @property
        def item(self):
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
