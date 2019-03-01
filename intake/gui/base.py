import panel as pn

class Base(object):  # pragma: no cover
    widget = None
    watchers = []  # list of all the watchers that we might want to disconnect

    def __repr__(self):
        return ("Intake GUI instance: to get widget to display, you must "
                "install panel. If running from within jupyterlab, "
                "install the jlab extension.")

    def unwatch(self):
        for watcher in self.watchers:
            self.widget.param.unwatch(watcher)

    def panel(self):
        return pn.Column(self.widget)
