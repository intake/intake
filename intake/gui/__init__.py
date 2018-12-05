#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

try:
    import ipywidgets
    from .widgets import DataBrowser

except ImportError:

    class DataBrowser(object):
        def __repr__(self):
            raise RuntimeError("Please install ipywidgets to use the Data "
                               "Browser")
