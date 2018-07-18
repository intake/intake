from intake.source.base import DataSource


class FooPlugin(DataSource):
    name = 'otherfoo'
    version = '0.1'
    container = 'dataframe'
    partition_access = False

    def __init__(self, **kwargs):
        pass
