#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
try:
    import panel as pn
    from ..gui import Base, SourceSelector, Description, DefinedPlots, logo_file

    class GUI(Base):
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
                        self.plot),
                    self.source_browser.panel,
                    self.description.panel,
                    background='#eeeeee',
                    sizing_mode='stretch_width'),
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

except ImportError:

    class GUI(object):
        def __repr__(self):
            raise RuntimeError("Please install panel to use the GUI")

except Exception as e:

    class GUI(object):
        def __repr__(self):
            raise RuntimeError("Initialisation of GUI failed, even though "
                               "panel is installed. Please update it "
                               "to a more recent version.")



