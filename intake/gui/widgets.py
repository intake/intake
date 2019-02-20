#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import OrderedDict
from contextlib import contextmanager
import ipywidgets as widgets
import os
import posixpath

import intake


here = os.path.abspath(os.path.dirname(__file__))
logo_file = os.path.join(here, 'logo.png')
logo = widgets.Box([widgets.Image.from_file(logo_file)],
                   layout=widgets.Layout(width='46px', height='35px',
                                         overflow_y='visible',
                                         overflow_x='visible'))


@contextmanager
def ignore(ob):
    try:
        ob.ignore = True
        yield
    finally:
        ob.ignore = False


class Base(object):  # pragma: no cover
    done_callback = None

    def stop(self, ok=False):
        done = self.done_callback is not None
        if ok and done:
            self.done_callback(ok)
        elif done:
            self.done_callback(None)

    def __repr__(self):
        return ("Intake GUI instance: to get widget to display, you must "
                "install ipy/jupyter-widgets, run in a notebook and, in "
                "the case of jupyter-lab, install the jlab extension.")

    def _ipython_display_(self, **kwargs):
        from IPython.core.interactiveshell import InteractiveShell
        import IPython

        # from IPython.Widget._ipython_display_
        if InteractiveShell.initialized():
            if self.widget._view_name is not None:

                plaintext = repr(self)
                data = {
                    'text/plain': plaintext,
                    'application/vnd.jupyter.widget-view+json': {
                        'version_major': 2,
                        'version_minor': 0,
                        'model_id': self.widget._model_id
                    }
                }
                IPython.display.display(data, raw=True)
                self.widget._handle_displayed(**kwargs)


class DataBrowser(Base):  # pragma: no cover
    """Intake data set browser

    Examples
    --------

    Display GUI in a notebook thus:

    >>> gui = intake.gui.widgets.DataBrowser()
    >>> gui.widget

    Optionally you can specify some Catalog instances to begin with. If none
    are given, will use the default one, ``intake.cat``.

    After interacting, the value of ``gui.item`` will be the entry selected.
    """

    def __init__(self, cats=None):
        if isinstance(cats, list):
            cats = cats
        elif cats is None:
            cats = [intake.cat]
        else:
            cats = [cats]
        self.cats = OrderedDict([(cat.name, cat) for cat in cats])
        self.cat_list = widgets.Select(rows=9)
        self.item = None
        self.ignore = False
        self.exception = None
        self.item_list = widgets.Select(rows=9)
        self.detail = widgets.Textarea(rows=7,
                                       placeholder='Item Description')
        self.add = widgets.Button(
            icon='plus-square',
            tooltip='Add Catalog',
            layout=widgets.Layout(width='auto'))
        self.add.on_click(self.open)
        self.search = widgets.Button(
            icon='search',
            tooltip='Search entries',
            layout=widgets.Layout(width='auto', height='auto',
                                  overflow_y='visible', overflow_x='visible'))
        self.search.on_click(self.opensearch)
        self.remove = widgets.Button(
            icon='minus-square',
            tooltip='Remove cat',
            layout=widgets.Layout(width='auto', height='auto',
                                  overflow_y='visible', overflow_x='visible'))
        self.remove.on_click(self.del_cat)
        self.logobox = widgets.VBox(
            children=[logo, self.search, self.add, self.remove],
            layout=widgets.Layout(width='auto',
                                  height='auto',
                                  overflow_y='visible',
                                  overflow_x='visible'))
        self.mid = widgets.HBox(children=[self.logobox, self.cat_list,
                                          self.item_list, self.detail])
        self.widget = widgets.VBox(children=[self.mid])
        self.widget.__repr__ = self.__repr__
        self.update_cat_list()
        self.cat_selected({'new': list(self.cats)[0]})
        self.cat_list.observe(self.cat_selected, 'value')
        self.item_list.observe(self.item_selected, 'value')

    def update_cat_list(self):
        with ignore(self):
            # only list cats with entries
            self.cat_list.options = [c for c in list(self.cats)
                                     if list(self.cats[c])]
            if len(self.cat_list.options) == 0:
                self.item_list.options = ()
                self.detail.value = ""

    def cat_selected(self, ev):
        name = ev['new']
        if self.ignore or name is None:
            return
        with ignore(self):
            names = [n + '  ->' if self.cats[name][n]._container == 'catalog'
                     else n for n in self.cats[name]]
            self.item_list.options = names
            if names:
                # avoid error if cat contains no entries
                self.item_selected({'new': names[0]}, first=True)

    def item_selected(self, ev, first=False):
        name = ev['new']
        if (self.ignore and not first) or name is None:
            return
        with ignore(self):
            if name.endswith('  ->'):
                if not first:
                    cat = self.cats[self.cat_list.value][name[:-4]]
                    self.cats[self.cat_list.value + '.' + cat.name] = cat()
                    self.update_cat_list()
            else:
                self.item = self.cats[self.cat_list.value][name]
                self.detail.value = "\n".join(
                    ["{}: {}".format(k, v) for k, v
                     in tuple(self.item.describe().items()) +
                     tuple(self.item.describe_open().items())])

    def add_cat(self, ev=None):
        """Add catalog instance to the browser

        Also called, without a catalog instance, when the (+) button is
        pressed, which will attempt to read a catalog and, if successful,
        add it to the browser.
        """
        if isinstance(ev, intake.catalog.Catalog):
            cat = ev
        else:
            try:
                cat = intake.open_catalog(ev)
                list(cat)  # fails if parse is bad
            except Exception as e:
                self.exception = e
                return
        self.cats[cat.name] = cat
        self.update_cat_list()
        self.cat_list.value = cat.name
        self.cat_selected({'new': cat.name})

    def del_cat(self, ev):
        if self.cat_list.value:
            self.cats.pop(self.cat_list.value)
        self.update_cat_list()
        if self.cat_list.options:
            cat = self.cat_list.options[-1]
            self.cat_list.value = cat
            self.cat_selected({'new': cat})

    def open(self, ev):
        cat_open = AddCat(self.file_chosen, filters=['.yaml', '.yml'])
        self.widget.children = [self.mid, cat_open.widget]

    def opensearch(self, ev):
        if self.cats:
            s = Search(done_callback=self.dosearch)
            self.widget.children = [self.mid, s.widget]

    def dosearch(self, out):
        if out:
            text, depth = out
            if len(depth) == 1:
                depth = int(depth)
            else:
                depth = 99
            cat = self.cats[self.cat_list.value]
            cat2 = cat.search(text, depth=depth)
            if len(list(cat2)):
                self.add_cat(cat2)
            self.widget.children = [self.mid]

    def file_chosen(self, fn):
        """Callback from AddCat panel"""
        if fn is not None:
            self.add_cat(fn)
        self.widget.children = [self.mid]


class Search(Base):  # pragma: no cover
    def __init__(self, done_callback=None):
        self.done_callback = done_callback
        self.label = widgets.Label(value='Search Text:')
        self.search = widgets.Text(
            placeholder="Set of words",
            layout=widgets.Layout(flex='10 1 auto', width='auto'))
        self.depth = widgets.Dropdown(description="Depth",
                                      options=['1', '2', '3', '4', '5', "All"])
        self.x = widgets.Button(
            icon='close', tooltip='Close Selector',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.x.on_click(lambda ev: self.stop())
        self.ok = widgets.Button(
            icon='check', tooltip='OK',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.ok.on_click(lambda ev: self.stop(ok=(self.search.value,
                                                  self.depth.value)))
        self.widget = widgets.HBox(children=[self.label, self.search,
                                             self.depth, self.ok,
                                             self.x])


class AddCat(Base):  # pragma: no cover
    """Sub-widget for adding new cats from file or remote"""

    def __init__(self, done_callback=None, filters=None):
        """
        Parameters
        ----------
        done_callback: function
            Called when the tick or cross buttons are clicked. Expects
            signature func(path, ok=True|False).
        """
        self.fs = FileSelector(done_callback=done_callback,
                               filters=filters or ['yaml', 'yml'])
        self.url = URLSelector(done_callback=done_callback)
        self.widget = widgets.Tab(children=[self.fs.widget, self.url.widget],)
        self.widget.set_title(0, "Local")
        self.widget.set_title(1, "Remote")


class URLSelector(Base):  # pragma: no cover
    def __init__(self, done_callback=None):
        self.done_callback = done_callback
        self.lurl = widgets.Label(value='URL:')
        self.url = widgets.Text(
            placeholder="Full URL with protocol",
            layout=widgets.Layout(flex='10 1 auto', width='auto'))
        self.x = widgets.Button(
            icon='close', tooltip='Close Selector',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.x.on_click(lambda ev: self.stop())
        self.ok = widgets.Button(
            icon='check', tooltip='OK',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.ok.on_click(lambda ev: self.stop(ok=self.url.value))
        self.widget = widgets.HBox(children=[self.lurl, self.url, self.ok,
                                             self.x])


class FileSelector(Base):  # pragma: no cover
    """
    ipywidgets interface for picking files

    The current path is stored in .path anf the current selection is stored in
    .label2.value.
    """
    def __init__(self, done_callback=None, filters=None):
        """

        Parameters
        ----------
        done_callback: function
            Called when the tick or cross buttons are clicked. Expects
            signature func(path, ok=True|False).
        filters: list of str or None
            Only show files ending in one of these strings. Normally used for
            picking file extensions. None is an alias for [''], passes all
            files.
        """
        self.done_callback = done_callback
        self.filters = [''] if filters is None else filters
        self.path = os.getcwd() + '/'
        self.main = widgets.Select(rows=15)
        self.button = widgets.Button(
            icon='chevron-left', tooltip='Parent',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.button.on_click(self.up)
        self.label = widgets.Label(
            layout=widgets.Layout(flex='100 1 auto', width='auto'))
        self.x = widgets.Button(
            icon='close', tooltip='Close Selector',
            layout=widgets.Layout(width='auto'))
        self.x.on_click(lambda ev: self.stop())
        self.ok = widgets.Button(
            icon='check', tooltip='OK',
            layout=widgets.Layout(width='auto'))
        self.ok.on_click(lambda ev: self._ok())
        self.make_options()
        self.main.observe(self.changed, 'value')
        self.upper = widgets.Box(children=[self.button, self.label])
        self.right = widgets.VBox(children=[self.x, self.ok])
        self.lower = widgets.HBox(children=[self.main, self.right])
        self.widget = widgets.VBox(children=[self.upper, self.lower])
        self.ignore = False

    def _ok(self):
        fn = self.main.value
        if fn.endswith('/'):
            self.stop()
        else:
            self.stop(os.path.join(self.path, fn))

    def make_options(self):
        self.ignore = True
        self.label.value = self.path
        out = []
        for f in sorted(os.listdir(self.path)):
            if os.path.isdir(self.path + f):
                out.append(f + '/')
            elif (not f.startswith('.') and any(f.endswith(ext)
                                                for ext in self.filters)):
                out.append(f)
        self.main.value = None
        self.main.options = out
        self.ignore = False

    def up(self, ev):
        self.path = os.path.dirname(
            self.path.rstrip('/')).rstrip('/') + '/'
        self.make_options()

    def changed(self, ev):
        if self.ignore:
            return
        with ignore(self):
            fn = ev['new']
            if fn.endswith('/'):
                self.path = self.path + fn
                self.make_options()
