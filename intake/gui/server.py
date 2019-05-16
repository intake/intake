#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2019, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
"""
The simplest possible panel server. To launch a panel server containing the intake gui
run:

panel serve intake/gui/server.py

"""

import intake
intake.gui.servable()
