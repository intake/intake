# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from intake.source.base import DataSource


class FooPlugin(DataSource):
    name = "otherfoo"
    version = "0.1"
    container = "dataframe"
    partition_access = False

    def __init__(self, **kwargs):
        pass
