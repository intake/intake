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


class BaseSelector(Base):
    """
    CHANGE
    """
    preprocess = None

    def _create_options(self, items):
        if not isinstance(items, list):
            items = [items]
        if self.preprocess:
            items = map(self.preprocess, items)
        return OrderedDict(map(lambda x: (x.name, x), items))

    @property
    def options(self):
        return self.widget.options

    @options.setter
    def options(self, new):
        print('setting options', new)
        options = self._create_options(new)
        print('new options', options)
        self.widget.options = options
        self.widget.value = list(options.values())[:1]

    def add(self, items):
        """Add items to options"""
        print('adding', items)
        options = self._create_options(items)
        new_options = self.widget.options
        if isinstance(new_options, dict):
            new_options.update(options)
        else:
            new_options = options
        print('new options', new_options)
        self.widget.options = new_options
        self.widget.value = list(options.values())[:1]

    def remove(self, items):
        """Remove items from options"""
        print('removing', items)
        if not isinstance(items, list):
            items = [items]
        new_options = self.widget.options
        for item in items:
            new_options.popitem(item)
        print('new options', new_options)
        self.widget.value = []
        self.widget.options = new_options if len(new_options) > 0 else []

    @property
    def selected(self):
        return self.widget.value

    @selected.setter
    def selected(self, new):
        print('selecting', new)
        if isinstance(new, str):
            new = [self.options[new]]
        elif hasattr(new, "name") and new in self.options.values():
            new = [new]
        print('new', new)
        self.widget.value = new


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
        self.options = self._cats
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
        print('cats', self._cats)
        if cats is not None and self.widget is not None:
            self.options = cats
            print(self.widget.options, self.widget.value)

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
        self.options = self._sources
        self.children = [self.widget]

    @property
    def cats(self):
        return set(source._catalog for source in self.sources)

    @cats.setter
    def cats(self, cats):
        """Set options from a list of cats"""
        print('setting cats', cats)
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
        print('setting sources', self._sources)
        if sources is not None and self.widget is not None:
            self.options = sources
            print(self.widget.options, self.widget.value)
