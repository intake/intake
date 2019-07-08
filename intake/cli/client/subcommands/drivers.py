#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
'''

'''

from __future__ import print_function

import logging
log = logging.getLogger(__name__)

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

# Standard library imports
import errno
from importlib import import_module
import inspect
import os
import sys

# External imports

# Intake imports
from intake import __version__
from intake.cli.util import Subcommand
from intake.source.discovery import all_enabled_drivers, enable, disable
from intake.config import confdir

#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

class Drivers(Subcommand):
    '''
    List, enable, and disable intake drivers.
    '''

    name = "drivers"

    def initialize(self):
        sub_parser = self.parser.add_subparsers()

        list = sub_parser.add_parser('list', help='Show all intake drivers.')
        list.add_argument('-v', '--verbose', action='store_true', help='Show module path.')
        list.set_defaults(invoke=self._list)

        enable = sub_parser.add_parser('enable', help='Enable one or more intake drivers.')
        enable.add_argument('drivers', type=str, help='Module path and class name, as in package.submodule.ClassName', nargs='+')
        enable.set_defaults(invoke=self._enable)

        disable = sub_parser.add_parser('disable', help='Disable one or more intake drivers.')
        disable.add_argument('drivers', type=str, help='Module path and class name, as in package.submodule.ClassName', nargs='+')
        disable.set_defaults(invoke=self._disable)

    def invoke(self, args):
        self.parser.print_help()

    def _list(self, args):
        if args.verbose:
            fmt = '{name:<30}{cls.__module__}.{cls.__name__} @ {file}'
        else:
            fmt = '{name:<30}{cls.__module__}.{cls.__name__}'
        for name, cls in all_enabled_drivers().items():
            print(fmt.format(name=name, cls=cls, file=inspect.getfile(cls)),
                  file=sys.stderr)

    def _enable(self, args):
        for driver in args.drivers:
            enable(driver)

    def _disable(self, args):
        for driver in args.drivers:
            disable(driver)
