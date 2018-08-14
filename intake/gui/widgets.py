from collections import OrderedDict
from contextlib import contextmanager
import ipywidgets as widgets
import os
import intake


@contextmanager
def ignore(ob):
    try:
        ob.ignore = True
        yield
    finally:
        ob.ignore = False


class DataBrowser(object):
    """Intake data set browser

    Usage
    -----
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
        self.cat_list = widgets.Select()
        self.update_cat_list()
        self.fs = None
        self.item = None
        self.ignore = False
        self.exception = None
        self.item_list = widgets.Select()
        self.detail = widgets.Textarea(disabled=True,
                                       placeholder='Item Description')
        self.add = widgets.Button(
            icon='plus-square',
            tooltip='Add Catalog',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.add.on_click(self.add_cat)
        self.lurl = widgets.Label(value='URL:')
        self.url = widgets.Text(value=os.getcwd(),
            layout=widgets.Layout(flex='10 1 auto', width='auto'))
        self.files = widgets.Button(
            icon='folder-open',
            tooltip='Open File Selector',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.files.on_click(self.openfs)
        self.mid = widgets.HBox(children=[self.cat_list,
                                          self.item_list, self.detail])
        self.bottom = widgets.HBox(children=[self.lurl, self.url, self.files,
                                             self.add])
        self.widget = widgets.VBox(children=[self.mid, self.bottom])
        self.cat_selected({'new': list(self.cats)[0]})
        self.cat_list.observe(self.cat_selected, 'value')
        self.item_list.observe(self.item_selected, 'value')

    def update_cat_list(self):
        with ignore(self):
            # only list cats with entries
            self.cat_list.options = [c for c in list(self.cats)
                                     if list(self.cats[c])]

    def cat_selected(self, ev):
        name = ev['new']
        if self.ignore or name is None:
            return
        with ignore(self):
            names = [n + '  ->' if self.cats[name][n].container == 'catalog'
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
                self.detail.value = str(self.item.describe())

    def add_cat(self, ev=None):
        """Add catalog instance to the browser

        Also called, without a catalog instance, when the (+) button is
        pressed, which will attempt to read a catalog and, if successful,
        add it to the browser.
        """
        if isinstance(ev, intake.catalog.Catalog):
            cat = ev
        else:
            fn = self.url.value
            try:
                cat = intake.open_catalog(fn)
                list(cat)  # fails if parse is bad
            except Exception as e:
                self.exception = e
                return
        self.cats[cat.name] = cat
        self.update_cat_list()

    def _ipython_display_(self):
        return self.widget._ipython_display_()

    def openfs(self, ev):
            self.fs = FileSelector(self.file_chosen, filters=['.yaml', '.yml'])
            self.widget.children = [self.mid, self.bottom, self.fs.selector]

    def file_chosen(self, fn, ok=True):
        if ok:
            self.url.value = fn
        self.widget.children = [self.mid, self.bottom]


class FileSelector(object):
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
        self.done = done_callback
        self.filters = [''] if filters is None else filters
        self.path = os.getcwd() + '/'
        self.widget = widgets.Select(rows=15)
        self.button = widgets.Button(
            icon='chevron-left', tooltip='Parent',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.button.on_click(self.up)
        self.label = widgets.Label(
            layout=widgets.Layout(flex='100 1 auto', width='auto'))
        self.x = widgets.Button(
            icon='close', tooltip='Close Selector',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.x.on_click(self.stop)
        self.button2 = widgets.Button(
            icon='check', tooltip='OK',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.button2.on_click(self.stop)
        self.label2 = widgets.Label(
            value='<no file selected>',
            layout=widgets.Layout(flex='100 1 auto', width='auto'))
        self.make_options()
        self.widget.observe(self.changed, 'value')
        self.upper = widgets.Box(children=[
            self.button, self.label
        ])
        self.lower = widgets.Box(children=[
            self.x, self.button2, self.label2
        ])
        self.selector = widgets.VBox(children=[
            self.upper, self.widget, self.lower
        ])
        self.ignore = False

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
        self.widget.value = None
        self.widget.options = out
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
            else:
                self.label2.value = self.path + fn

    def stop(self, ev):
        self.widget.unobserve_all()
        self.button.unobserve_all()
        self.button2.unobserve_all()
        self.x.unobserve_all()
        ok = ev.tooltip == 'OK' and self.label2.value != '<no file selected>'
        if self.done is not None:
            self.done(self.label2.value, ok=ok)
