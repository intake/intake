from collections import OrderedDict
import ipywidgets as widgets
import os
import intake


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
        self.item_list = widgets.Select()
        self.add = widgets.Button(icon='plus-square',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.add.on_click(self.add_cat)
        self.url = widgets.Text(value='Catalog URL',
            layout=widgets.Layout(flex='10 1 auto', width='auto'))
        self.files = widgets.Button(icon='folder-open',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.files.on_click(self.openfs)
        self.mid = widgets.HBox(children=[self.cat_list, self.item_list])
        self.bottom = widgets.HBox(children=[self.url, self.files, self.add])
        self.widget = widgets.VBox(children=[self.mid, self.bottom])
        self.cat_selected({'new': list(self.cats)[0]})
        self.cat_list.observe(self.cat_selected, 'value')
        self.item_list.observe(self.item_selected, 'value')
        self.item = None

    def update_cat_list(self):
        self.cat_list.options = list(self.cats)

    def cat_selected(self, ev):
        name = ev['new']
        if name is None:
            return
        names = [n + '  ->' if self.cats[name][n].container == 'catalog'
                 else n for n in self.cats[name]]
        self.item_list.options = names

    def item_selected(self, ev):
        name = ev['new']
        if name is None:
            return
        if name.endswith('  ->'):
            cat = self.cats[self.cat_list.value][name[:-4]]
            self.cats[self.cat_list.value + '.' + cat.name] = cat()
            self.update_cat_list()
        else:
            self.item = self.cats[self.cat_list.value][name]

    def add_cat(self, ev):
        fn = self.url.value
        try:
            cat = intake.open_catalog(fn)
            self.cats[cat.name] = cat
            self.update_cat_list()
        except Exception as e:
            print(fn, e)

    def openfs(self, ev):
        self.fs = FileSelector(self.file_chosen)
        self.widget.children = [self.mid, self.bottom, self.fs.selector]

    def file_chosen(self, fn):
        self.url.value = fn
        self.widget.children = [self.mid, self.bottom]


class FileSelector(object):
    def __init__(self, done_callback=None):
        self.done = done_callback
        self.path = os.getcwd() + '/'
        self.widget = widgets.Select(rows=15)
        self.button = widgets.Button(icon='chevron-left',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.button.on_click(self.up)
        self.label = widgets.Label(
            layout=widgets.Layout(flex='100 1 auto', width='auto'))
        self.button2 = widgets.Button(icon='check',
            layout=widgets.Layout(flex='1 1 auto', width='auto'))
        self.button2.on_click(self.stop)
        self.label2 = widgets.Label(value='<no file setected>',
            layout=widgets.Layout(flex='100 1 auto', width='auto'))
        self.make_options()
        self.widget.observe(self.changed, 'value')
        self.upper = widgets.Box(children=[
            self.button, self.label
        ])
        self.lower = widgets.Box(children=[
            self.button2, self.label2
        ])
        self.selector = widgets.VBox(children=[
            self.upper, self.widget, self.lower
        ])
        self.ignore = False

    def make_options(self):
        self.ignore = True
        self.label.value = self.path
        out = [(f + '/') if os.path.isdir(self.path + f) else f
               for f in sorted(os.listdir(self.path))
               if not f.startswith('.')]
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
        self.widget.disabled = True
        self.button.disabled = True
        self.button2.disabled = True
        if self.done is not None:
            self.done(self.label2.value)
