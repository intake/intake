# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import panel as pn

import intake
from intake.interface.base import ICONS
from intake.interface.catalog.add import CatAdder
from intake.interface.catalog.search import Search
from intake.interface.source import defined_plots


class GUI:
    """
    Top level GUI panel

    This class is responsible for coordinating the inputs and outputs
    of various sup-panels and their effects on each other.

    Parameters
    ----------
    cats: dict of catalogs
        catalogs used to initalize the cat panel, {display_name: cat_object}

    """

    def __init__(self, cats=None):
        # state
        self._children = {}  # cat name in the selector to child catalogs' names: cat objects
        # mapping of name in the selector to catalog object
        self._cats = cats or {"builtin": intake.cat}
        self._sources = {}  # source name: source instance

        # layout
        col0 = pn.Column(pn.pane.PNG(ICONS["logo"], align="center"), margin=(25, 0, 0, 0), width=50)
        self.catsel = pn.widgets.MultiSelect(
            name="Catalogs",
            options=list(self._cats),
            value=[],
            size=13,
            styles={"width": "25%"},
        )
        self.catsel.param.watch(self.cat_selected, "value")
        add = pn.widgets.Button(name="+")
        sub = pn.widgets.Button(name="-")
        search = pn.widgets.Button(name="🔍")
        col1 = pn.Column(self.catsel, pn.Row(add, sub, search))
        add.on_click(self.add_clicked)
        sub.on_click(self.sub_clicked)
        search.on_click(self.search_clicked)

        self.sourcesel = pn.widgets.MultiSelect(name="Sources", size=13, styles={"width": "25%"})
        plot = pn.widgets.Button(name="📊")
        plot.on_click(self.plot_clicked)
        self.sourcesel.param.watch(self.source_selected, "value")
        col2 = pn.Column(self.sourcesel, plot)

        self.sourceinf = pn.widgets.CodeEditor(
            readonly=True, language="yaml", print_margin=False, annotations=[]
        )
        col3 = pn.Column(self.sourceinf)

        row0 = pn.Row(col0, col1, col2, col3, styles={"width": "100%"})

        self.plots = defined_plots.Plots()
        self.plots.panel.visible = False
        self.add = CatAdder(done_callback=self.add_catalog)
        self.add.panel.visible = False
        self.search = Search(done_callback=self.searched)
        self.search.panel.visible = False
        self.row1 = pn.Row(self.plots.panel, self.add.panel, self.search.panel)

        self.main = pn.Column(row0, self.row1)
        self.cat_selected(None)

    def _repr_mimebundle_(self, *args, **kwargs):
        """Display in a notebook or a server"""
        return self.main._repr_mimebundle_(*args, **kwargs)

    def show(self, *args, **kwargs):
        return self.main.show(*args, **kwargs)

    def __repr__(self):
        return "Intake GUI"

    def cat_selected(self, *_):
        right = "└─>"

        cat = self.catsel.value
        if not cat:
            return
        else:
            catname = cat[0]
            cat = self._cats[catname]
        catsel_needs_update = False
        self._sources.clear()
        indent = len(catname) - len(catname.lstrip(" ")) + 2
        for entry in cat:
            name = " " * indent + right + entry
            source = cat[entry]
            if isinstance(source, intake.catalog.Catalog):
                if name not in self._cats:
                    self._cats[name] = source
                    self._children.setdefault(catname, []).append(name)
                    catsel_needs_update = True
            elif "Catalog" in getattr(source, "output_instance", ""):
                if name not in self._cats:
                    cat = source.read()
                    self._cats[name] = cat
                    self._children.setdefault(catname, []).append(name)
                    catsel_needs_update = True
            else:
                self._sources[entry] = source
        if catsel_needs_update:
            self.update_catsel()
        self.sourcesel.param.update(options=list(self._sources))

    def update_catsel(self):
        self.catsel.param.update(options=get_catlist(self._cats, self._children))

    def add_catalog(self, cat, name=None, **_):
        if hasattr(cat, "token"):
            if "CATALOG_PATH" in cat.user_parameters:
                par = cat.user_parameters["CATALOG_PATH"]
                name = getattr(par, "default", str(par))
            else:
                name = cat.token
        else:
            name = name or getattr(cat, "token", cat.name)
        self._cats[name] = cat
        self.update_catsel()

    def source_selected(self, *_):
        from intake import BaseReader
        import yaml

        source = self.sourcesel.value
        if not source:
            return
        else:
            source = self._sources[source[0]]
        if isinstance(source, BaseReader):
            # could have reverted to ReaderDescription, but this version will include any
            # other readers/data, not just references.
            d = {"cls": source.qname()}
            d.update(source.to_dict())
            txt = yaml.dump(d, default_flow_style=False)
        else:
            txt = yaml.dump(source._yaml()["sources"], default_flow_style=False)
        self.sourceinf.param.update(value=txt)

    def plot_clicked(self, *_):
        if self.plots.panel.visible:
            self.plots.panel.visible = False
        elif self.sources:
            self.plots.source = self.sources[0]
            self.add.panel.visible = False
            self.plots.panel.visible = True
            self.search.panel.visible = False

    def searched(self, searchstring: str):
        if self.cats:
            cat = self.cats[0]
            cat2 = cat.search(searchstring)
            self.add_catalog(cat2, name=f"search <{searchstring[:10]}>")

    def add_clicked(self, *_):
        if self.add.panel.visible:
            self.add.panel.visible = False
        else:
            self.add.panel.visible = True
            self.plots.panel.visible = False
            self.search.panel.visible = False

    def sub_clicked(self, *_):
        for catname in self.catsel.value:
            self.remove_cat(catname)

    def remove_cat(self, catname, done=True):
        self._cats.pop(catname, None)  # remake "builtin" if accidentally removed?
        for cat in self._children.get(catname, []):
            self.remove_cat(cat, done=False)
        if done:
            self.catsel.param.update(options=list(self._cats))

    def search_clicked(self, *_):
        if self.search.panel.visible:
            self.search.panel.visible = False
        else:
            self.add.panel.visible = False
            self.plots.panel.visible = False
            self.search.panel.visible = True

    @property
    def cats(self):
        """Cats that have been selected from the cat sub-panel"""
        return [self._cats[k] for k in self.catsel.value]

    @property
    def sources(self):
        """Sources that have been selected from the source sub-panel"""
        return [self._sources[k] for k in self.sourcesel.value]

    @property
    def source_instance(self):
        return self.sources[0] if self.sourcesel.values else None


def get_catlist(catnames, children, outlist=None, seen=None):
    outlist = outlist or []
    seen = seen or set()
    for name in sorted(catnames):
        if name in seen:
            continue
        seen.add(name)
        outlist.append(name)
        if name in children:
            get_catlist(children[name], children, outlist, seen)
    return outlist
