from ...source import base, registry


def assert_items_equal(a, b):
    assert len(a) == len(b) and sorted(a) == sorted(b)


class TestingSource(base.DataSource):
    """A source that gives back whatever parameters were passed to it"""
    name = 'test'
    version = '0.0.1'
    container = 'python'
    partition_access = False

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super(TestingSource, self).__init__('python')
        self.npartitions = 1

    def _load_metadata(self):
        pass

    def _get_partition(self, _):
        return self.args, self.kwargs


def register():
    registry['test'] = TestingSource
