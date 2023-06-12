# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import panel as pn


class Search:
    """Search panel for searching a list of catalogs

    Parameters
    ----------
    done_callback: function with cats as input
        function that is called when new cats have been generated
        via the search functionality
    """

    def __init__(self, done_callback: callable):
        self.done_callback = done_callback
        self.tinput = pn.widgets.TextInput()
        ok = pn.widgets.Button(name="OK")
        ok.on_click(self.go)
        label = pn.widgets.StaticText(value="Search")
        self.panel = pn.Row(label, self.tinput, ok)

    def go(self, *_):
        if self.tinput.value:
            self.done_callback(self.tinput.value)
