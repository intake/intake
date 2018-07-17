from intake.source.base import DataSource


class Ex2Plugin(DataSource):
    name = 'example2',
    version = '0.1',
    container = 'dataframe',
    partition_access = True

    def __init__(self):
        super(Ex2Plugin, self).__init__()
