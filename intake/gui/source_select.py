#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import OrderedDict

import intake
import panel as pn

from .base import Base

def coerce_to_list(items, preprocess=None):
    """Given an instance or list, coerce to list.

    With optional preprocessing.
    """
    if not isinstance(items, list):
        items = [items]
    if preprocess:
        items = list(map(preprocess, items))
    return items


class BaseSelector(Base):
    """Base class for capturing selector logic.

    Parameters
    ----------
    preprocess: function
        run on every input value when creating options
    widget: panel widget
        selector widget which this class keeps uptodate with class properties
    """
    preprocess = None
    widget = None

    @property
    def items(self):
        """Available items to select from"""
        return list(self.options.values())

    @items.setter
    def items(self, items):
        """When setting items, enable any dependent widgets and make
        sure widget options are uptodate"""
        if items is not None:
            self.options = items
        self.enable_dependents(items)

    def _create_options(self, items):
        """Helper method to create options from list, or instance.

        Applies preprocess method if available to create a uniform
        output
        """
        return OrderedDict(map(lambda x: (x.name, x),
                           coerce_to_list(items, self.preprocess)))

    @property
    def options(self):
        """Options available on the widget"""
        return self.widget.options

    @options.setter
    def options(self, new):
        """Set options from list, or instance of named item

        Over-writes old options
        """
        options = self._create_options(new)
        self.widget.options = options
        self.widget.value = list(options.values())[:1]

    def add(self, items):
        """Add items to options"""
        options = self._create_options(items)
        self.widget.options.update(options)
        self.widget.param.trigger('options')
        self.widget.value = list(options.values())[:1]

    def remove(self, items):
        """Remove items from options"""
        values = coerce_to_list(items)
        for value in values:
            self.widget.options.pop(value.name)
        self.widget.param.trigger('options')

    @property
    def selected(self):
        """Value sepected on the widget"""
        return self.widget.value

    @selected.setter
    def selected(self, new):
        """Set selected from list or instance of object or name.

        Over-writes existing selection
        """
        def preprocess(item):
            if isinstance(item, str):
                return self.options[item]
            return item
        items = coerce_to_list(new, preprocess)
        self.widget.value = items


class CatSelector(BaseSelector):
    """
    The cat selector takes a variety of inputs such as a catalog instance,
    a path to a catalog, or a list of either of those.

    Once the cat selector is populated with these options, the user can
    select which catalog(s) are of interest. These catalogs are stored on
    the ``selected`` property of the class.
    """
    children = []

    def __init__(self, cats=None, **kwargs):
        """Set cats to initialize the class.

        The order of the calls in this method matters and is different
        from the order in other panel init methods because the top level
        gui class needs to be able to watch these widgets.
        """
        self.panel = pn.Column(name='Select Catalog')
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        super().__init__(**kwargs)

        self.items = cats if cats is not None else [intake.cat]

    def setup(self):
        self.remove_button = pn.widgets.Button(
            name='Remove Selected Catalog',
            width=200)
        self.dependent_widgets.append(self.remove_button)

        self.watchers = [
            self.remove_button.param.watch(self.remove_selected, 'clicks'),
            self.widget.param.watch(self.enable_dependents, 'value'),
        ]

        self.children = [self.widget, self.remove_button]

    def teardown(self):
        self.dependent_widgets.remove(self.remove_button)

    def preprocess(self, cat):
        """Function to run on each cat input"""
        if isinstance(cat, str):
            cat = intake.open_catalog(cat)
        return cat

    def remove_selected(self, *args):
        """Remove the selected catalog - allow the passing of arbitrary
        args so that buttons work"""
        self.remove(self.selected)


class SourceSelector(BaseSelector):
    """
    The source selector takes a variety of inputs such as cats or sources
    and uses those to populate a select widget containing all the sources.

    Once the source selector is populated with these options, the user can
    select which source(s) are of interest. These sources are stored on
    the ``selected`` property of the class.
    """
    preprocess = None
    children = []

    def __init__(self, sources=None, cats=None, **kwargs):
        """Set sources or cats to initialize the class - sources trumps cats.

        The order of the calls in this method matters and is different
        from the order in other panel init methods because the top level
        gui class needs to be able to watch these widgets.
        """
        self.panel = pn.Column(name='Select Data Source')
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        super().__init__(**kwargs)

        if sources is not None:
            self.items = sources
        elif cats is not None:
            self.cats = cats

    def setup(self):
        self.watchers = [
            self.widget.param.watch(self.enable_dependents, 'value'),
        ]

        self.children = [self.widget]

    @property
    def cats(self):
        """Available cats to select from"""
        return set(source._catalog for source in self.items)

    @cats.setter
    def cats(self, cats):
        """Set sources from a list of cats"""
        sources = []
        for cat in coerce_to_list(cats):
            sources.extend(list(cat._entries.values()))
        self.items = sources
