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
        return ("Intake GUI instance: to get widget to display, you must "
                "install panel. If running from within jupyterlab, "
                "install the jlab extension.")

    def setup(self):
        """This method should set self.children"""
        raise NotImplementedError

    @property
    def visible(self):
        return self._visible

    @visible.setter
    def visible(self, visible):
        if visible and len(self.panel.objects) == 0:
            self.setup()
            self.panel.extend(self.children)
        elif not visible and len(self.panel.objects) > 0:
            self.teardown()
            self.panel.clear()
        self._visible = visible

    def teardown(self):
        """This method should get rid of any lingering watchers"""
        if self.widget:
            for watcher in self.watchers:
                self.widget.param.unwatch(watcher)
                self.watchers.remove(watcher)
