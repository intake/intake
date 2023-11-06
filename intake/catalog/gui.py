# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

try:
    from ..interface.gui import GUI

except ImportError:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass

        def __repr__(self):
            raise RuntimeError(
                "Please install panel to use the GUI (`conda "
                "install -c conda-forge panel>0.8.0`)"
            )

except Exception:

    class GUI(object):
        def __init__(self, *args, **kwargs):
            pass

        def __repr__(self):
            raise RuntimeError(
                "Initialization of GUI failed, even though "
                "panel is installed. Please update it "
                "to a more recent version (`conda install -c "
                "conda-forge panel==0.5.1`)."
            )
