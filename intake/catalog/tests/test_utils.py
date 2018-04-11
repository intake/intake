from .. import utils
from ...source import base


def test_make_prefix_tree():
    x = {'abc.xyz': 1, 'abc.def': 2, 'abc.www.yyy': 3, 'www': 4}
    assert utils.make_prefix_tree(x) == \
        {'abc': {'xyz': 1, 'def': 2, 'www': {'yyy': 3}}, 'www': 4}


class Plugin(base.Plugin):
    """A plugin that gives back whatever parameters were passed to it"""
    def __init__(self):
        super(Plugin, self).__init__(name='test',
                                     version=0,
                                     container='python',
                                     partition_access=False)

    def open(self, *args, **kwargs):
        return TestingSource(*args, **kwargs)


class TestingSource(base.DataSource):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def describe(self):
        return self.args, self.kwargs

    def read(self):
        return self.args, self.kwargs
