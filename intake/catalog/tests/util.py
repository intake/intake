from ...source import base


def assert_items_equal(a, b):
    assert len(a) == len(b) and sorted(a) == sorted(b)


class Plugin(base.Plugin):
    """A plugin that makes a TestSource"""
    def __init__(self):
        super(Plugin, self).__init__(name='test',
                                     version=0,
                                     container='python',
                                     partition_access=False)

    def open(self, *args, **kwargs):
        return TestingSource(*args, **kwargs)


class TestingSource(base.DataSource):
    """A source that gives back whatever parameters were passed to it"""
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def describe(self):
        return self.args, self.kwargs

    def read(self):
        return self.args, self.kwargs
