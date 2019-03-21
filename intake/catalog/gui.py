#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
try:
    import panel as pn
    from ..gui import *

    class CatalogGUI(Base):
        def __init__(self, cat, **kwargs):
            self.cat = cat
            self.panel = pn.Column(name='GUI')
            super().__init__(**kwargs)

        def setup(self):
            self.plot = pn.widgets.RadioButtonGroup(
                options={'📊': True, 'x': False},
                value=False,
                width=80)

            self.source_browser = SourceSelector(cats=[self.cat],
                                                 dependent_widgets=[self.plot])
            self.description = Description(source=self.sources)
            self.plotter = DefinedPlots(source=self.sources,
                                        control_widget=self.plot)

            self.watchers = [
                self.plot.param.watch(self.on_click_plot, 'value'),
                self.source_browser.widget.link(self.description, value='source'),
            ]

            self.children = [
                pn.Row(
                    pn.Column(
                        logo_file,
                        self.plot
                    ),
                    self.source_browser.panel,
                    self.description.panel,
                    background=BACKGROUND,
                    width_policy='max',
                    max_width=MAX_WIDTH,
                ),
                self.plotter.panel,
            ]

        def on_click_plot(self, event):
            """ When the plot control is toggled, set visibility and hand down source"""
            self.plotter.source = self.sources
            self.plotter.visible = event.new
            if self.plotter.visible:
                self.plotter.watchers.append(
                    self.source_browser.widget.link(self.plotter, value='source'))

        @property
        def sources(self):
            return self.source_browser.selected

        @property
        def item(self):
            if len(self.sources) == 0:
                return None
            return self.sources[0]

    class EntryGUI(Base):
        def __init__(self, source, **kwargs):
            self.source = source
            self.panel = pn.Column(name='GUI')
            super().__init__(**kwargs)

        def setup(self):
            self.plot = pn.widgets.RadioButtonGroup(
                options={'📊': True, 'x': False},
                value=False,
                disabled=len(self.source.plots) == 0,
                width=80)

            self.description = Description(source=self.source)
            self.plotter = DefinedPlots(source=self.source,
                                        control_widget=self.plot)

            self.watchers = [
                self.plot.link(self.plotter, value='visible'),
            ]

            self.children = [
                pn.Row(
                    pn.Column(
                        logo_file,
                        self.plot
                    ),
                    self.description.panel,
                    background=BACKGROUND,
                    width_policy='max',
                    max_width=MAX_WIDTH,
                ),
                self.plotter.panel,
            ]

        @property
        def item(self):
            return self.source

except ImportError:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass
        def __repr__(self):
            raise RuntimeError("Please install panel to use the GUI")

    EntryGUI = GUI
    CatalogGUI = GUI


except Exception as e:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass
        def __repr__(self):
            raise RuntimeError("Initialization of GUI failed, even though "
                               "panel is installed. Please update it "
                               "to a more recent version.")
    EntryGUI = GUI
    CatalogGUI = GUI
