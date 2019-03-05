import panel as pn

def pretty_describe(object, nestedness=0, indent=2):
    """Maintain dict ordering - but make string version prettier"""
    if not isinstance(object, dict):
        return str(object)
    sep = f'\n{" " * nestedness * indent}'
    return sep.join((f'{k}: {pretty_describe(v, nestedness + 1)}' for k, v in object.items()))


class Description(object):
    def __init__(self, source=None):
        self._source = source
        self.pane = pn.pane.Str(self.contents, sizing_mode='stretch_width')

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

    def from_sources(self, sources):
        if len(sources) > 0:
            source = sources[0]
            self.source = source
        else:
            self.source = None

    @property
    def contents(self):
        if not self._source:
            return ' '
        contents = self.source.describe().copy()
        try:
            contents.update(self.source.describe_open())
            return pretty_describe(contents)
        except ValueError:
            warning = f'Need additional plugin to use {self.source._driver} driver'
            return pretty_describe(contents) + '\n' + warning

    def panel(self):
        return pn.Column(self.pane)
