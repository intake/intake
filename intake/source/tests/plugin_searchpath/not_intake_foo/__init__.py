from intake.source.base import Plugin


class OtherFooPlugin(Plugin):
    def __init__(self):
        super(OtherFooPlugin, self).__init__(name='otherfoo', version='0.1',
                                             container='dataframe',
                                             partition_access=False)

    def open(self, **kwargs):
        return 'open_worked'  # Don't actually use this plugin
