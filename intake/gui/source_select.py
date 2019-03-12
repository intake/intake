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
    options = None
    allow_next = None

    def callback(self, *events):
        print(self.panel.name, events)
        for event in events:
            if event.name == 'value' and event.new != self.selected:
                self.select(event.new)
            if self.allow_next is not None:
                self.allow_next(self.selected)

    def update_selected(self):
        self.widget.value = self.selected

    def update_options(self):
        self.widget.options = self.options.copy()

    def add(self, items, autoselect=True):
        """Add items to the options and select the new ones"""
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
    children = []

    def __init__(self, cats=None, visible=True):
        if cats is None:
            cats = [intake.cat]
        self.cats = cats
        self.panel = pn.Column(name='Select Catalog')
        self.visible = visible

    def setup(self):
        self.options = {}
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        self.add(self.cats)
        self.remove_button = pn.widgets.Button(
            name='Remove Selected Catalog',
            width=200)

        self.watchers = [
            self.widget.param.watch(self.callback, ['value']),
            self.remove_button.param.watch(self.remove_selected, 'clicks')
        ]

        self.children = [self.widget, self.remove_button]

    def allow_next(self, allow):
        self.remove_button.disabled = not allow

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
    preprocess = None
    children = []

    def __init__(self, sources=None, cats=None, visible=True):
        self.sources = sources
        if sources is None and cats is not None:
            self.cats = cats
        self.panel = pn.Column(name='Select Data Source')
        self.visible = visible


    def setup(self):
        self.options = {}
        self.widget = pn.widgets.MultiSelect(size=9, width=200)
        if self.sources is not None:
            self.add(self.sources)

        self.watchers = [
            self.widget.param.watch(self.callback, ['value'])
        ]

        self.children = [self.widget]

    @property
    def cats(self):
        return set(source._catalog for source in self.options.values())

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
