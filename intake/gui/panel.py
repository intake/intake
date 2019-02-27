#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import OrderedDict
from contextlib import contextmanager
import panel as pn
import os
import posixpath

import intake


here = os.path.abspath('~/intake/intake/gui')
logo_file = os.path.join(here, 'logo.png')
logo = pn.Column(logo_file)


def cat_from_string_or_cat(cat):
    """Return a catalog instance when given a cat or a string"""
    if isinstance(cat, str):
        cat = intake.open_catalog(cat)
    return cat

def cats_from_string_list_or_cats(cats):
    if isinstance(cats, dict):
        return cats
    if isinstance(cats, list):
        cats = cats
    else:
        cats = [cats]
    cats = map(cat_from_string_or_cat, cats)
    return OrderedDict([(cat.name, cat) for cat in cats])


class Base(object):  # pragma: no cover
    widget = None
    selected = None
    options = None
    watchers = []  # list of all the watchers that we might want to disconnect

    def __repr__(self):
        return ("Intake GUI instance: to get widget to display, you must "
                "install panel. If running from within jupyterlab, "
                "install the jlab extension.")

    def unwatch(self):
        for watcher in self.watchers:
            self.widget.param.unwatch(watcher)

    def callback(self, *events):
        print(events)
        for event in events:
            if event.name == 'options' and event.new != self.options:
                self.add(event.new)
            elif event.name == 'value' and event.new != self.selected:
                self.select(event.new)

    def update_selected(self):
        if self.widget.value != self.selected:
            self.widget.value = self.selected

    def update_options(self):
        if self.widget.options != self.options:
            self.widget.options = self.options.copy()

    def add(self, new):
        raise NotImplementedError

    def select(self, new):
        raise NotImplementedError

    def panel(self):
        return pn.Column(self.widget)


class CatalogBrowser(Base):  # pragma: no cover
    """Intake Catalog browser

    Examples
    --------

    Display GUI in a notebook thus:

    >>> gui = intake.gui.widgets.CatalogBrowser()
    >>> gui.panel()

    Optionally you can specify some Catalog instances to begin with. If none
    are given, will use the default one, ``intake.cat``.

    After interacting, the value of ``gui.item`` will be the entry selected.
    """
    selected = []  # list because it is a multi-select

    def __init__(self, cats=None):
        if cats is None:
            cats = [intake.cat]
        self.options = cats_from_string_list_or_cats(cats)
        self.widget = pn.widgets.MultiSelect(size=9)
        self.update_options()
        self.watchers.append(
            self.widget.param.watch(self.callback, ['options', 'value']))

    def select(self, cat):
        """Select one cat by name"""
        if isinstance(cat, str):
            cat = [self.options[cat]]
        self.selected = cat
        self.update_selected()

    def unselect(self):
        cats = self.selected
        self.selected = []
        self.update_selected()
        return cats

    def add(self, cat):
        options = cats_from_string_list_or_cats(cat)
        self.options.update(options)
        self.update_options()
        self.select(list(options.values()))

    def del_selected(self):
        cats = self.unselect()
        for cat in cats:
            self.options.pop(cat.name)
        self.update_options()
