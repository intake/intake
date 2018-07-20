from intake.source.base import DataSource


class ExampleSource(DataSource):
    name = 'example1'
    version = '0.1'
    container = 'dataframe'
    partition_access = True

    def __init__(self, **kwargs):
        super(ExampleSource, self).__init__()
