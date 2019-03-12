#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import panel as pn

class Base(object):  # pragma: no cover
    children = None
    widget = None
    panel = None
    watchers  = None
    _visible = True

    def __repr__(self):
        try:
            return self.panel.__repr__()
        except:
            raise RuntimeError("Panel does not seem to be set up properly")


    def _repr_mimebundle_(self, *args, **kwargs):
        try:
            return self.panel._repr_mimebundle_(*args, **kwargs)
        except:
            raise RuntimeError("Panel does not seem to be set up properly")

    def setup(self):
        """This method should set self.children"""
        raise NotImplementedError


    def enable_widget(self, enable=True):
        if self.widget is not None:
            self.widget.disabled = not enable

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
        if visible and len(self.panel.objects) == 0:
            self.setup()
            self.panel.extend(self.children)
        elif not visible and len(self.panel.objects) > 0:
            self.unwatch()
            self.panel.clear()
        self._visible = visible

    def unwatch(self):
        """This method should get rid of any lingering watchers"""
        if self.watchers is not None:
            for watcher in self.watchers:
                watcher.inst.param.unwatch(watcher)
                self.watchers.remove(watcher)
