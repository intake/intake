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
except Exception as e:

    class DataBrowser(object):
        def __repr__(self):
            raise RuntimeError("Initialisation of GUI failed, even though "
                               "ipywidgets is installed. Please update it "
                               "to a more recent version.")


class InstanceMaker(object):

    def __init__(self, cls, *args, **kwargs):
        self._cls = cls
        self._args = args
        self._kwargs = kwargs
        self._instance = None

    def _instantiate(self):
        if self._instance is None:
            self._instance = self._cls(*self._args, **self._kwargs)

    def __getattr__(self, attr, *args, **kwargs):
        self._instantiate()
        return getattr(self._instance, attr, *args, **kwargs)

    def __getitem__(self, item):
        self._instantiate()
        return self._instance[item]

    def __repr__(self):
        self._instantiate()
        return repr(self._instance)

    def __dir__(self):
        self._instantiate()
        return dir(self._instance)
