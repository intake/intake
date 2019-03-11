#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os

import intake
import panel as pn

from .base import Base

here = os.path.abspath(os.path.dirname(__file__))
ICONS = {
    'check': os.path.join(here, 'icons', 'baseline-check-24px.svg'),
    'error': os.path.join(here, 'icons', 'baseline-error-24px.svg'),
}

class FileSelector(Base):
    """
    Panel interface for picking files

    The current path is stored in .path and the current selection is stored in
    .url. Currently does not support windows style paths
    """
    def __init__(self, filters=['yaml', 'yml'], visible=True):
        self.filters = filters
        self.panel = pn.Column(name='Local')
        self.visible = visible


    def setup(self):
        self.path_text = pn.widgets.TextInput(value=os.getcwd() + '/')
        self.validator = pn.pane.SVG(ICONS['check'])
        self.path_text.param.watch(self.validate, ['value'])

        self.main = pn.widgets.MultiSelect(size=15)
        self.path_text.param.watch(self.make_options, ['value'])

        self.home = pn.widgets.Button(name='🏠', width=40, height=30)
        self.home.param.watch(self.go_home, 'clicks')
        self.up = pn.widgets.Button(name='‹', width=30, height=30)
        self.up.param.watch(self.move_up, 'clicks')

        self.make_options()
        self.main.param.watch(self.move_down, ['value'])

        self.children = [
            pn.Row(self.home, self.up, self.path_text, self.validator),
            self.main
        ]

    @property
    def path(self):
        return self.path_text.value

    @property
    def url(self):
        return os.path.join(self.path, self.main.value[0])

    def move_up(self, arg=None):
        self.path_text.value = os.path.dirname(self.path.rstrip('/')).rstrip('/') + '/'

    def go_home(self, arg=None):
        self.path_text.value = os.getcwd() + '/'

    def validate(self, arg=None):
        if not self.path_text.value.endswith('/'):
            self.path_text.value += '/'
        if os.path.isdir(self.path_text.value):
            self.validator.object = ICONS['check']
        else:
            self.validator.object = ICONS['error']

    def make_options(self, arg=None):
        out = []
        for f in sorted(os.listdir(self.path)):
            if f.startswith('.'):
                continue
            elif os.path.isdir(self.path + f):
                out.append(f + '/')
            elif any(f.endswith(ext) for ext in self.filters):
                out.append(f)

        self.main.value = []
        self.main.options = dict(zip(out, out))

    def move_down(self, *events):
        for event in events:
            if event.name == 'value' and len(event.new) > 0:
                fn = event.new[0]
                if fn.endswith('/'):
                    self.path_text.value = self.path + fn
                    self.make_options()

class URLSelector(Base):
    """
    Panel interface for inputting a URL to a remote catalog

    The inputted URL is stored in .url.
    """
    def __init__(self, visible=True):
        self.panel = pn.Row(name='Remote')
        self.visible = visible

    def setup(self):
        self.watchers = []
        self.label = 'URL:'
        self.widget = pn.widgets.TextInput(
            placeholder="Full URL with protocol",
            width=600)
        self.children = ['URL:', self.widget]

    @property
    def url(self):
        return self.widget.value


class CatAdder(Base):
    """Sub-widget for adding new cats from file or remote"""

    def __init__(self, visible=True, done_callback=None):
        self.done_callback = done_callback
        self.panel = pn.Column(name='Add Catalog')
        self.visible = visible

    def setup(self):
        self.watchers = []
        self.fs = FileSelector()
        self.url = URLSelector()
        self.selectors = [self.fs, self.url]
        self.tabs = pn.Tabs(*map(lambda x: x.panel, self.selectors))
        self.widget = pn.widgets.Button(name='Add Catalog')
        self.watchers.append(
            self.widget.param.watch(self.add_cat, 'clicks'))
        self.children = [self.tabs, self.widget]

    @property
    def cat_url(self):
        return self.selectors[self.tabs.active].url

    @property
    def cat(self):
        # might want to do some validation in here
        return intake.open_catalog(self.cat_url)

    def add_cat(self, arg=None):
        if self.cat.name is not None:
            self.done_callback(self.cat)
