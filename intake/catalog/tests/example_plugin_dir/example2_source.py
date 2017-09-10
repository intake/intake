from intake.source.base import Plugin


class Ex2Plugin(Plugin):
    def __init__(self):
        super().__init__(name='example2', version='0.1', container='dataframe', partition_access=True)
