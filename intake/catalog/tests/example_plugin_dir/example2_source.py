#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from intake.source.base import DataSource


class Ex2Plugin(DataSource):
    name = 'example2'
    version = '0.1'
    container = 'dataframe'
    partition_access = True

    def __init__(self):
        super(Ex2Plugin, self).__init__()
