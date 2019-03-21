#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os

import intake
import panel as pn

from .base import Base, MAX_WIDTH, BACKGROUND

here = os.path.abspath(os.path.dirname(__file__))
ICONS = {
    'check': os.path.join(here, 'icons', 'baseline-check-24px.svg'),
    'error': os.path.join(here, 'icons', 'baseline-error-24px.svg'),
}

class FileSelector(Base):
    """
    Panel interface for picking files

    The current path is stored in .path and the current selection is stored in
    .url.

    Parameters
    ----------
    filters: list of string
        extentions that are included in the list of files - correspond to
        catalog extensions.
    """
    def __init__(self, filters=['yaml', 'yml'], **kwargs):
        self.filters = filters
        self.panel = pn.Column(name='Local')
        super().__init__(**kwargs)

    def setup(self):
        self.path_text = pn.widgets.TextInput(value=os.getcwd() + os.path.sep,
                                              width_policy='max')
        self.validator = pn.pane.SVG(ICONS['check'], width=25)
        self.main = pn.widgets.MultiSelect(size=15, width_policy='max')
        self.home = pn.widgets.Button(name='🏠', width=40, height=30)
        self.up = pn.widgets.Button(name='‹', width=30, height=30)

        self.make_options()

        self.watchers = [
            self.path_text.param.watch(self.validate, ['value']),
            self.path_text.param.watch(self.make_options, ['value']),
            self.home.param.watch(self.go_home, 'clicks'),
            self.up.param.watch(self.move_up, 'clicks'),
            self.main.param.watch(self.move_down, ['value'])
        ]

        self.children = [
            pn.Row(self.home, self.up, self.path_text, self.validator),
            self.main
        ]

    @property
    def path(self):
        path = self.path_text.value
        return path if path.endswith(os.path.sep) else path + os.path.sep

    @property
    def url(self):
        return os.path.join(self.path, self.main.value[0])

    def move_up(self, arg=None):
        self.path_text.value = os.path.dirname(self.path.rstrip(os.path.sep)) + os.path.sep

    def go_home(self, arg=None):
        self.path_text.value = os.getcwd() + os.path.sep

    def validate(self, arg=None):
        """Check that inputted path is valid - set validator accordingly"""
        if os.path.isdir(self.path):
            self.validator.object = ICONS['check']
        else:
            self.validator.object = ICONS['error']

    def make_options(self, arg=None):
        self.enable_dependents(False)
        out = []
        if os.path.isdir(self.path):
            for f in sorted(os.listdir(self.path)):
                if f.startswith('.'):
                    continue
                elif os.path.isdir(os.path.join(self.path, f)):
                    out.append(f + os.path.sep)
                elif any(f.endswith(ext) for ext in self.filters):
                    out.append(f)

        self.main.value = []
        self.main.options = dict(zip(out, out))

    def move_down(self, *events):
        for event in events:
            if event.name == 'value' and len(event.new) > 0:
                fn = event.new[0]
                if fn.endswith(os.path.sep):
                    self.path_text.value = self.path + fn
                    self.make_options()
                elif os.path.isfile(self.url):
                    self.enable_dependents(True)

class URLSelector(Base):
    """
    Panel interface for inputting a URL to a remote catalog

    The inputted URL is stored in .url.
    """
    def __init__(self, **kwargs):
        self.panel = pn.Row(name='Remote',
                            width_policy='max')
        super().__init__(**kwargs)

    def setup(self):
        self.main = pn.widgets.TextInput(
            placeholder="Full URL with protocol",
            width_policy='max')
        self.children = ['URL:', self.main]

    @property
    def url(self):
        return self.main.value


class CatAdder(Base):
    """Panel for adding new cats from file or remote

    Parameters
    ----------
    done_callback: function with cat as input
        function that is called when the "Add Catalog" button is clicked.
    """

    def __init__(self, done_callback=None, **kwargs):
        self.done_callback = done_callback
        self.panel = pn.Column(name='Add Catalog', background=BACKGROUND,
                               width_policy='max', max_width=MAX_WIDTH)
        super().__init__(**kwargs)

    def setup(self):
        self.widget = pn.widgets.Button(name='Add Catalog',
                                        disabled=True,
                                        width_policy='min')
        self.fs = FileSelector(dependent_widgets=[self.widget])
        self.url = URLSelector()
        self.selectors = [self.fs, self.url]
        self.tabs = pn.Tabs(*map(lambda x: x.panel, self.selectors))

        self.watchers = [
            self.widget.param.watch(self.add_cat, 'clicks'),
            self.tabs.param.watch(self.tab_change, 'active'),
        ]

        self.children = [self.tabs, self.widget]

    @property
    def cat_url(self):
        return self.selectors[self.tabs.active].url

    @property
    def cat(self):
        # might want to do some validation in here
        return intake.open_catalog(self.cat_url)

    def add_cat(self, arg=None):
        """Add cat and close panel"""
        if self.cat.name is not None:
            self.done_callback(self.cat)

    def tab_change(self, event):
        """Enable widget when on URL tab"""
        if event.new == 1:
            self.widget.disabled = False
