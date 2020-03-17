#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
"""
CLI for listing, enabling, disabling intake drivers
"""


# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

# Standard library imports
import inspect
import sys

# External imports

# Intake imports
from intake import __version__
from intake.cli.util import Subcommand
from intake.source.discovery import autodiscover, autodiscover_all, enable, disable
from intake.config import confdir
import intake

import logging
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------


class Drivers(Subcommand):
    """
    List, enable, and disable intake drivers.
    """

    name = "drivers"

    def initialize(self):
        sub_parser = self.parser.add_subparsers()

        list = sub_parser.add_parser(
            'list',
            help='Show all intake drivers, whether enabled, disabled, '
                 'or directly inserted into the registry'
        )
        list.add_argument(
            '-v', '--verbose', action='store_true', help='Show module path.')
        list.set_defaults(invoke=self._list)

        enable = sub_parser.add_parser('enable', help='Enable an intake driver.')
        enable.add_argument('name', type=str, help='Driver name')
        enable.add_argument('driver', type=str,
                            help='Module path and class name, as in '
                                 'package.submodule.ClassName')
        enable.set_defaults(invoke=self._enable)

        disable = sub_parser.add_parser(
            'disable', help='Disable one or more intake drivers.')
        disable.add_argument('names', type=str, help='Driver names', nargs='+')
        disable.set_defaults(invoke=self._disable)

    def invoke(self, args):
        self.parser.print_help()

    def _list(self, args):
        if args.verbose:
            fmt = '{name:<30}{cls.__module__}.{cls.__name__} @ {file}'
        else:
            fmt = '{name:<30}{cls.__module__}.{cls.__name__}'
        drivers_by_name = autodiscover()   # dict mapping name to driver
        all_drivers = autodiscover_all()  # listof (name, driver)
        direct = {k: v for k, v in intake.registry.items()
                  if k not in all_drivers and k not in drivers_by_name}

        print("Direct:", file=sys.stderr)
        none = True
        for name in sorted(direct, key=str):
            cls = direct[name]
            print(fmt.format(name=str(name), cls=cls, file=inspect.getfile(cls)),
                  file=sys.stderr)
            none = False
        if none:
            print("<none>")

        print("\nEnabled:", file=sys.stderr)
        none = True
        for name in sorted(drivers_by_name, key=str):
            cls = drivers_by_name[name]
            print(fmt.format(name=str(name), cls=cls, file=inspect.getfile(cls)),
                  file=sys.stderr)
            none = False
        if none:
            print("<none>")

        print("\nNot enabled:", file=sys.stderr)
        none = True
        for name, cls in sorted(all_drivers, key=lambda x: str(x[0])):
            if drivers_by_name.get(name, None) is not cls:
                print(fmt.format(name=str(name), cls=cls, file=inspect.getfile(cls)),
                      file=sys.stderr)
                none = False
        if none:
            print("<none>")

    def _enable(self, args):
        enable(args.name, args.driver)

    def _disable(self, args):
        for name in args.names:
            disable(name)
