#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
'''

'''

import logging
log = logging.getLogger(__name__)

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

# Standard library imports
from importlib import import_module
import sys

# External imports

# Intake imports
from intake import __version__
from intake.cli.util import Subcommand

#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

class Info(Subcommand):
    ''' Display runtime information related to Intake

    '''

    name = "info"

    def initialize(self):
        pass

    def invoke(self, args):
        print("Python version      :  %s" % sys.version.split('\n')[0])
        print("IPython version     :  %s" % _version_from_module('IPython'))
        print("Tornado version     :  %s" % _version_from_module('tornado', 'version'))
        print("Dask version        :  %s" % _version_from_module('dask'))
        print("Pandas version      :  %s" % _version_from_module('pandas'))
        print("Numpy version       :  %s" % _version_from_module('numpy'))
        print("Intake version      :  %s" % __version__)

def _version_from_module(modname, version_attr="__version__"):
    try:
        mod = import_module(modname)
        return getattr(mod, version_attr)
    except ImportError:
        return "(not installed)"
