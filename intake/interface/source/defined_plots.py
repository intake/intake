# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2022, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import hvplot
from packaging.version import Version

try:
    import xrviz
    from xrviz.dashboard import Dashboard as XRViz

    assert Version(xrviz.__version__) >= Version("0.1.1")
except ImportError:
    xrviz = False

import panel as pn

from ...catalog.local import LocalCatalogEntry, YAMLFileCatalog
from ..base import BaseView


class Event:
    def __init__(self, plot, **kwargs):
        self.object = plot
        for key, val in kwargs.items():
            setattr(self, key, val)


class Plots(BaseView):
    """
    Panel for displaying pre-defined plots from catalog.

    Parameters
    ----------
    source: intake catalog entry, or list of same
        source to describe in this object
    edit_callback: callback to alert that plot has been edited

    Attributes
    ----------
    has_plots: bool
        whether the source has plots defined
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
        self.custom = pn.widgets.Button(name="Create", width=70, align="center")
        self.source = source
        self.panel = pn.Column(name="Plot", width_policy="max", margin=0)
        self._behavior_callback = None
        self._callbacks = []
        super().__init__(**kwargs)

    def setup(self):
        self.instructions = pn.pane.Markdown("**Select Plot**", align="center", width=70)
        self.select = pn.widgets.Select(options=self.options, height=30, align="center", min_width=200)
        # Add spaces in front of each option to make it render correctly.
        # This is a bit of a hack to make a Select which only displays the down arrow.
        self.edit_options = pn.widgets.Select(options=["  Edit", "  Clone", "  Rename", "  Delete"], align="center", width=25, margin=(5, -10))
        self.pane = pn.pane.HoloViews(self._plot_object(self.selected), name="Plot")

        self.interact_label = pn.pane.Markdown("Name", align="center", width_policy="max", min_width=100)
        self.interact_name = pn.widgets.TextInput(placeholder="Name of new plot...")
        self.interact_cancel = pn.widgets.Button(name="Cancel", width=100)
        self.interact_save = pn.widgets.Button(name="Save", button_type="primary", width=100)

        self.watchers = [
            self.select.param.watch(self.plot_selected, ["value"]),
            self.custom.param.watch(self.interact, ["clicks"]),
            self.edit_options.param.watch(self.interact, ["value"]),
            self.interact_name.param.watch(self.name_changed, ["value_input"]),
            self.interact_cancel.param.watch(self.cancel, ["clicks"]),
            self.interact_save.param.watch(self.interact_action, ["clicks"]),
        ]
        self.alert = pn.pane.Alert(alert_type="danger")
        self.out = pn.Row(self.pane, name="Plot")

        self.row_select_plots = pn.Row(
            self.instructions,
            self.select,
            self.custom,
            self.edit_options,
        )
        self.row_dialog_buttons = pn.Row(
            self.interact_label,
            self.interact_name,
            self.interact_cancel,
            self.interact_save,
        )
        self.children = [
            pn.Column(
                self.row_select_plots,
                self.row_dialog_buttons,
                pn.Row(self.alert),
                self.out,
            )
        ]
        # Set initial visibility
        self.row_select_plots[-1].visible = False  # edit_options dropdown
        self.row_dialog_buttons.visible = False
        self.alert.visible = False

    @BaseView.source.setter
    def source(self, source):
        """When the source gets updated, update the options in
        the selector"""
        if source and isinstance(source, list):
            source = source[0]
        if isinstance(source, LocalCatalogEntry):
            source = source()
        BaseView.source.fset(self, source)
        if self.select:
            self.select.options = self.options
        if source and source.container == "dataframe":
            self.custom.disabled = False
        elif source and xrviz and source.container in ["xarray", "ndarray", "numpy"]:
            self.custom.disabled = False
        else:
            self.custom.disabled = True

    @property
    def has_plots(self):
        """Whether the source has plots defined"""
        return self.source is not None and len(self._source.plots) > 0

    @property
    def options(self):
        """Plots options defined on the source"""
        return (["None"] + self.source.plots) if self.source is not None else []

    @property
    def selected(self):
        """Name of selected plot"""
        return self.select.value if self.select is not None else None

    @selected.setter
    def selected(self, selected):
        """When plot is selected set, make sure widget stays upto date"""
        self.select.value = selected

    def watch(self, callback):
        self._callbacks.append(callback)

    def plot_selected(self, *events):
        for event in events:
            if event.name == "value":
                self.pane.object = self._plot_object(event.new)
            self.custom.name = "Create" if str(self.select.value) == "None" else "Edit"
            self.edit_options.visible = self.custom.name == "Edit"

    def name_changed(self, *events):
        for event in events:
            if event.name == "value_input":
                self.alert.visible = False
                # Empty name not allowed and name cannot already be in use
                if len(event.new) == 0:
                    self.interact_save.disabled = True
                elif event.new in self.options:
                    self.interact_save.disabled = True
                    self.alert.object = f'Name "{event.new}" already exists'
                    self.alert.visible = True
                else:
                    self.interact_save.disabled = False

    def cancel(self, _):
        self.pane.object = self._plot_object(self.selected)
        self.out[0] = self.pane
        self.row_select_plots.visible = True
        self.row_dialog_buttons.visible = False
        self._behavior_callback = None

    def interact_action(self, _):
        # Stop catalog from autoreloading
        self.source.cat.ttl = None
        if self._behavior_callback is not None:
            # Perform action
            event = self._behavior_callback()
            # Save catalog (if YAMLFileCatalog)
            if isinstance(self.source.cat, YAMLFileCatalog):
                self.source.cat.add(self.source)
            # Callbacks
            for cb in self._callbacks:
                cb(event)
        # Reset custom behavior callback
        self._behavior_callback = None
        # End action
        self.cancel(None)

    def interact(self, _):
        # Create/Edit/Clone/Rename/Delete was selected
        if self._behavior_callback is not None:
            # This is a short-circuit to allow edit options reset
            # without causing an infinite loop
            return
        if self.selected == "None":
            # Create
            self._behavior_callback = self._create
            self.interact_save.name = "Save"
            self.interact_save.disabled = True
            self.interact_label.object = "Name "
            self.interact_name.value = ""
            self.interact_name.visible = True
            viz = self.draw()
            self.out[0] = viz
        elif self.edit_options.value == "  Clone":
            self._behavior_callback = self._clone
            self.interact_save.name = "Clone"
            self.interact_save.disabled = True
            self.interact_label.object = f'Clone "**{self.selected}**" as '
            self.interact_name.value = ""
            self.interact_name.visible = True
        elif self.edit_options.value == "  Rename":
            self._behavior_callback = self._rename
            self.interact_save.name = "Rename"
            self.interact_save.disabled = True
            self.interact_label.object = f'Rename "**{self.selected}**" to '
            self.interact_name.value = ""
            self.interact_name.visible = True
        elif self.edit_options.value == "  Delete":
            self._behavior_callback = self._delete
            self.interact_save.name = "Delete"
            self.interact_save.disabled = False
            self.interact_label.object = f'Really delete "**{self.selected}**" ?'
            self.interact_name.visible = False
        elif self.edit_options.value == "  Edit":
            # Edit
            self._behavior_callback = self._edit
            self.interact_save.name = "Save"
            self.interact_save.disabled = False
            self.interact_label.object = f'Editing "**{self.selected}**"'
            self.interact_name.visible = False
            viz = self.draw()
            self.out[0] = viz
        else:
            raise ValueError(self.edit_options.value)
        # Update visibility of components
        self.row_dialog_buttons.visible = True
        self.row_select_plots.visible = False
        # Reset edit options selection (won't trigger
        # edit action because _behavior_callback is set)
        self.edit_options.value = "Edit"

    def draw(self):
        if self.selected == "None":
            kwargs = {"y": []}
        else:
            kwargs = self.source.metadata["plots"][self.selected]
        if self.source.container == "dataframe":
            df = self.source.to_dask()
            if df.npartitions == 1:
                df = df.compute()
            self._viz = viz = hvplot.explorer(df, **kwargs)
        elif self.source.container in ["xarray", "ndarray", "numpy"]:
            import xarray

            try:
                data = self.source.to_dask()
            except NotImplementedError:
                data = self.source.read()
            if not isinstance(data, (xarray.DataArray, xarray.Dataset)):
                data = xarray.DataArray(data)
            self._viz = XRViz(data, **kwargs)
            viz = self._viz.panel
        else:
            raise ValueError(f"Unhandled container type {self.source.container}")
        return viz

    def _plot_object(self, selected):
        if selected and str(selected) != "None":
            plot_method = getattr(self.source.plot, selected)
            self.out[0] = self.pane
            if plot_method:
                return plot_method()

    def _create(self):
        plot_name = self.interact_name.value
        # Add plot metadata to both DataSource and CatalogEntry
        self.source.metadata.setdefault("plots", {})[plot_name] = self._viz.settings()
        self.source.entry._metadata.setdefault("plots", {})[plot_name] = self._viz.settings()
        # Add new plot name to self.options
        self.select.options = self.options
        # Select new graph
        self.selected = plot_name
        return Event(self, kind="create", plot_name=plot_name)

    def _edit(self):
        # Update plot metadata for both DataSource and CatalogEntry
        self.source.metadata["plots"][self.selected] = self._viz.settings()
        self.source.entry._metadata["plots"][self.selected] = self._viz.settings()
        return Event(self, kind="edit", plot_name=self.selected)

    def _clone(self):
        plot_name = self.interact_name.value
        # Clone plot metadata for both DataSource and CatalogEntry
        self.source.metadata["plots"][plot_name] = self.source.metadata["plots"][self.selected].copy()
        self.source.entry._metadata["plots"][plot_name] = self.source.entry._metadata["plots"][self.selected].copy()
        # Add new plot name to self.options
        self.select.options = self.options
        # Select new graph
        self.selected = plot_name
        return Event(self, kind="clone", plot_name=plot_name, cloned_from=self.selected)

    def _rename(self):
        plot_name = self.interact_name.value
        # Rename plot metadata for both DataSource and CatalogEntry
        self.source.metadata["plots"][plot_name] = self.source.metadata["plots"].pop(self.selected)
        self.source.entry._metadata["plots"][plot_name] = self.source.entry._metadata["plots"].pop(self.selected)
        # Update available options in dropdown
        self.select.options = self.options
        # Select renamed graph
        self.selected = plot_name
        return Event(self, kind="rename", plot_name=plot_name, prev_name=self.selected)

    def _delete(self):
        # Delete plot metadata from both DataSource and CatalogEntry
        del self.source.metadata["plots"][self.selected]
        del self.source.entry._metadata["plots"][self.selected]
        # Update available options in dropdown
        self.select.options = self.options
        self.selected = "None"
        return Event(self, kind="delete", plot_name=self.selected)

    def __getstate__(self, include_source=True):
        """Serialize the current state of the object. Set include_source
        to False when using with another panel that will include source."""
        state = super().__getstate__(include_source)
        state.update(
            {
                "selected": self.selected,
            }
        )
        return state

    def __setstate__(self, state):
        """Set the current state of the object from the serialized version.
        Works inplace. See ``__getstate__`` to get serialized version and
        ``from_state`` to create a new object."""
        super().__setstate__(state)
        if self.visible:
            self.selected = state.get("selected")
        return self
