import intake
import panel as pn

class Description(object):
    def __init__(self, source=None):
        self._source = source
        self.pane = pn.pane.Markdown(self.contents)

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        """When the source gets updated, update the pane object"""
        if source != self._source:
            self._source = source
            self.pane.object = self.contents
        return self._source

    @property
    def contents(self):
        return str(self._source.describe()) if self._source else " "

    def panel(self):
        return pn.Column(self.pane)
