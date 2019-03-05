import panel as pn

class Base(object):  # pragma: no cover
    children = None
    widget = None
    panel = None
    watchers  = None

    def __repr__(self):
        return ("Intake GUI instance: to get widget to display, you must "
                "install panel. If running from within jupyterlab, "
                "install the jlab extension.")

    def unwatch(self):
        for watcher in self.watchers:
            self.widget.param.unwatch(watcher)

    def setup(self):
        """This method should set self.children"""
        raise NotImplementedError

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        if state == 'open' and len(self.panel.objects) == 0:
            self.setup()
            # self.panel.extend(self.children)  # after panel#279
            for child in self.children:
                self.panel.append(child)
        elif state == 'shut' and len(self.panel.objects) > 0:
            if self.widget:
                self.unwatch()
            # self.panel.clear()  # after panel#279
            for child in self.panel:
                self.panel.pop(child)
        self._state = state
