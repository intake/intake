from intake.source.base import Plugin, DataSource


class ExamplePlugin(Plugin):
    def __init__(self):
        super(ExamplePlugin, self).__init__(name='example1',
                                            version='0.1',
                                            container='dataframe',
                                            partition_access=True)

    def open(self, *args, **kwargs):
        return ExampleSource


class ExampleSource(DataSource):
    def __init__(self):
        super(ExampleSource, self).__init__(container='dataframe')
