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
    preprocess = None
    options = None

    def callback(self, *events):
        print(events)
        for event in events:
            if event.name == 'value' and event.new != self.selected:
                self.select(event.new)

    def update_selected(self):
        if self.widget.value != self.selected:
            self.widget.value = self.selected

    def update_options(self):
        if self.widget.options != self.options:
            self.widget.options = self.options.copy()

    def add(self, items, autoselect=True):
        """Add items to the options and select the new ones"""
        if isinstance(items, OrderedDict):
            options = items
        elif isinstance(items, dict):
            options = OrderedDict(items.items())
        else:
            if not isinstance(items, list):
                items = [items]
            if self.preprocess:
                items = map(self.preprocess, items)
            options = OrderedDict(map(lambda x: (x.name, x), items))
        self.options.update(options)
        self.update_options()
        self.select(list(options.values()))

    def remove(self, items):
        """Unselect items from options and remove them"""
        if not isinstance(items, list):
            items = [items]
        if len(list(filter(lambda x: x.name in self.options, items))) == 0:
            return
        self.unselect(items)
        for item in items:
            self.options.pop(item.name)
        self.update_options()

    def select(self, new):
        """Select one item by name or object"""
        if isinstance(new, str):
            new = [self.options[new]]
        elif hasattr(new, "name") and new in self.options.values():
            new = [new]
        if new != self.selected:
            self.selected = new
            self.update_selected()

    def unselect(self, old=None):
        """Unselect provided items or selected items"""
        if not old:
            old = self.selected
            self.selected = []
        elif isinstance(old, list):
            self.selected = [s for s in self.selected if s not in old]
        else:
            self.selected = [s for s in self.selected if s != old]
        self.update_selected()
        return old


class CatSelector(BaseSelector):
    selected = []
    options = {}
    children = []

    def __init__(self, cats=None, visible=True):
        if cats is None:
            cats = [intake.cat]
        self.cats = cats
        self.panel = pn.Column()
        self.visible = visible

    def setup(self):
        self.watchers = []
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        self.add(self.cats)
        self.watchers.append(
            self.widget.param.watch(self.callback, ['value']))

        self.remove_button = pn.widgets.Button(
            name='Remove Selected Catalog',
            width=200)
        self.remove_button.param.watch(self.remove_selected, 'clicks')

        self.children = [self.widget, self.remove_button]

    def preprocess(self, cat):
        if isinstance(cat, str):
            cat = intake.open_catalog(cat)
        return cat

    def remove_selected(self, *args):
        """Remove the selected catalog - allow the passing of arbitrary
        args so that buttons work"""
        self.remove(self.selected)


class SourceSelector(BaseSelector):
    selected = []
    options = {}
    preprocess = None
    children = []

    def __init__(self, sources=None, cats=None, visible=True):
        self.sources = sources
        if sources is None and cats is not None:
            self.cats = cats
        self.panel = pn.Column()
        self.visible = visible


    def setup(self):
        self.watchers = []
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        if self.sources is not None:
            self.add(self.sources)
        self.watchers.append(
            self.widget.param.watch(self.callback, ['value']))
        self.children = [self.widget]

    @property
    def cats(self):
        return [source._catalog for source in self.options.values()]

    @cats.setter
    def cats(self, cats):
        """Set options from a list of cats"""
        options = {}
        for cat in cats:
            options.update(OrderedDict([(s, cat[s]) for s in cat]))
        self.sources = list(options.values())
        if self.widget:
            self.options = options
            self.update_options()

            if list(options.values()):
                self.select([list(options.values())[0]])
            else:
                self.select([])
