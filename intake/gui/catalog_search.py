import panel as pn

from .base import Base


class Search(Base):
    """Input is a list of catalogs and output is a list of sources"""
    def __init__(self, cats, visible=True, done_callback=None):
        self.panel = pn.Row()
        self.visible = visible
        self.done_callback = done_callback
        self.cats = cats

    def setup(self):
        self.watchers = []
        self.text = pn.widgets.TextInput(
            placeholder="Set of words",
            width=400)
        self.depth = pn.widgets.Select(
            options=['1', '2', '3', '4', '5', "All"],
            width=80)
        self.widget = pn.widgets.Button(name='🔍', width=30)
        self.watchers.append(
            self.widget.param.watch(self.do_search, 'clicks'))

        self.children = ['Search Text:', self.text,
                         'Depth:', self.depth,
                         self.widget]

    def do_search(self, arg=None):
        search_text = self.text.value
        depth = int(self.depth.value) if len(self.depth.value) != "All" else 99

        new_cats = []
        for cat in self.cats:
            new_cat = cat.search(search_text, depth=depth)
            if len(list(new_cat)) > 0:
                new_cats.append(new_cat)
        if len(new_cats) > 0:
            self.done_callback(new_cats)
