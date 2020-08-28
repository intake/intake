#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from distutils.version import LooseVersion

try:
    import dfviz
    assert LooseVersion(dfviz.__version__) >= LooseVersion("0.1.0")
except ImportError:
    dfviz = False

try:
    import xrviz
    from xrviz.dashboard import Dashboard as XRViz
    assert LooseVersion(xrviz.__version__) >= LooseVersion("0.1.1")
except ImportError:
    xrviz = False

import panel as pn
from ..base import BaseView
from ...catalog.local import LocalCatalogEntry


class Plots(BaseView):
    """
    Panel for displaying pre-defined plots from catalog.

    Parameters
    ----------
    source: intake catalog entry, or list of same
        source to describe in this object

    Attributes
    ----------
    has_plots: bool
        whether the source has plots defined
    instructions_contents: str
        instructions to put on the plot selector label
    options: list
        plots options defined on the source
    selected: str
        name of selected plot
    children: list of panel objects
        children that will be used to populate the panel when visible
    panel: panel layout object
        instance of a panel layout (row or column) that contains children
        when visible
    watchers: list of param watchers
        watchers that are set on children - cleaned up when visible
        is set to false.
    """
    select = None

    def __init__(self, source=None, **kwargs):
        self.custom = pn.widgets.Button(name='Customize...', width_policy='min', align='center')
        self.source = source
        self.panel = pn.Column(name='Plot', width_policy='max', margin=0)
        super().__init__(**kwargs)

    def setup(self):
        self.instructions = pn.pane.Markdown(
            self.instructions_contents, align='center', width_policy='max')
        self.select = pn.widgets.Select(options=self.options, height=30,
                                        align='center', min_width=200)
        self.pane = pn.pane.HoloViews(self._plot_object(self.selected),
                                      name="Plot")

        self.watchers = [
            self.select.param.watch(self.callback, ['options', 'value']),
            self.custom.param.watch(self.interact, ['clicks'])
        ]
        self.out = pn.Row(self.pane, name="Plot")

        self.children = [
            pn.Row(
                self.instructions,
                self.select,
                self.custom
            ),
            self.out,
        ]

    @BaseView.source.setter
    def source(self, source):
        """When the source gets updated, update the the options in
        the selector"""
        if source and isinstance(source, list):
            source = source[0]
        if isinstance(source, LocalCatalogEntry):
            source = source()
        BaseView.source.fset(self, source)
        if self.select:
            self.select.options = self.options
        if source and dfviz and source.container == 'dataframe':
            self.custom.disabled = False
        elif source and xrviz and source.container in ['xarray', 'ndarray', 'numpy']:
            self.custom.disabled = False
        else:
            self.custom.disabled = True

    @property
    def has_plots(self):
        """Whether the source has plots defined"""
        return self.source is not None and len(self._source.plots) > 0

    @property
    def instructions_contents(self):
        """Instructions to put on the plot selector label"""
        if self.has_plots:
            return '**Select from the predefined plots:**'
        return '*No predefined plots found - declare these in the catalog*'

    @property
    def options(self):
        """Plots options defined on the source"""
        return (['None'] + self.source.plots) if self.source is not None else []

    @property
    def selected(self):
        """Name of selected plot"""
        return self.select.value if self.select is not None else None

    @selected.setter
    def selected(self, selected):
        """When plot is selected set, make sure widget stays upto date"""
        self.select.value = selected

    def callback(self, *events):
        for event in events:
            if event.name == 'value':
                self.pane.object = self._plot_object(event.new)
            if event.name == 'options':
                self.instructions.object = self.instructions_contents

    def interact(self, _):
        # "customize" was pressed
        if self.selected == 'None':
            kwargs = {}
        else:
            kwargs = self.source.metadata['plots'][self.selected]
        if self.source.container == 'dataframe':
            df = self.source.to_dask()
            if df.npartitions == 1:
                df = df.compute()
            viz = dfviz.DFViz(df, **kwargs)
        elif self.source.container in ['xarray', 'ndarray', 'numpy']:
            import xarray
            try:
                data = self.source.to_dask()
            except NotImplemented:
                data = self.source.read()
            if not isinstance(data, (xarray.DataArray, xarray.Dataset)):
                data = xarray.DataArray(data)
            viz = XRViz(data, **kwargs)
        else:
            return
        self.out[0] = viz.panel

    def _plot_object(self, selected):
        if selected and str(selected) != "None":
            plot_method = getattr(self.source.plot, selected)
            self.out[0] = self.pane
            if plot_method:
                return plot_method()

    def __getstate__(self, include_source=True):
        """Serialize the current state of the object. Set include_source
        to False when using with another panel that will include source."""
        state = super().__getstate__(include_source)
        state.update({
            'selected': self.selected,
        })
        return state

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        super().__setstate__(state)
        if self.visible:
            self.selected = state.get('selected')
        return self
