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
    preprocess = None

    def _create_options(self, items):
        """Helper method to create options from list, or instance.

        Applies preprocess method if available to create a uniform
        output
        """
        return OrderedDict(map(lambda x: (x.name, x),
                           coerce_to_list(items, self.preprocess)))

    @property
    def options(self):
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
        print(values, self.options)
        for value in values:
            self.widget.options.pop(value.name)
        self.widget.param.trigger('options')

    @property
    def selected(self):
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
    the selected property of the class.
    """
    children = []

    def __init__(self, cats=None, visible=True):
        if cats is None:
            cats = [intake.cat]
        self._cats = cats
        self.panel = pn.Column(name='Select Catalog')
        self.visible = visible

    def setup(self):
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        self.cats = self._cats
        self.remove_button = pn.widgets.Button(
            name='Remove Selected Catalog',
            width=200)

        self.watchers = [
            self.remove_button.param.watch(self.remove_selected, 'clicks'),
            self.widget.param.watch(self.allow_next, 'value'),
        ]

        self.children = [self.widget, self.remove_button]

    def allow_next(self, event):
        self.remove_button.disabled = len(event.new) == 0

    def preprocess(self, cat):
        if isinstance(cat, str):
            cat = intake.open_catalog(cat)
        return cat

    @property
    def cats(self):
        return list(self.options.values()) or self._cats

    @cats.setter
    def cats(self, cats):
        self._cats = cats
        if cats is not None and self.widget is not None:
            self.options = cats

    def remove_selected(self, *args):
        """Remove the selected catalog - allow the passing of arbitrary
        args so that buttons work"""
        self.remove(self.selected)


class SourceSelector(BaseSelector):
    preprocess = None
    children = []

    def __init__(self, sources=None, cats=None, visible=True):
        self._sources = sources
        if sources is None and cats is not None:
            self.cats = cats
        self.panel = pn.Column(name='Select Data Source')
        self.visible = visible

    def setup(self):
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        self.sources = self._sources
        self.children = [self.widget]

    @property
    def cats(self):
        return set(source._catalog for source in self.sources)

    @cats.setter
    def cats(self, cats):
        """Set options from a list of cats"""
        sources = []
        for cat in cats:
            sources.extend(list(cat._entries.values()))
        self.sources = sources

    @property
    def sources(self):
        return list(self.options.values()) or self._sources

    @sources.setter
    def sources(self, sources):
        self._sources = sources
        if sources is not None and self.widget is not None:
            self.options = sources
