from intake.source.base import Plugin


class FooPlugin(Plugin):
    def __init__(self):
        super(FooPlugin, self).__init__(name='foo', version='0.1',
                                        container='dataframe',
                                        partition_access=False)

    def open(self, **kwargs):
        return 'open_worked'  # Don't actually use this plugin
