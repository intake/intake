# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
"""
CLI for listing, enabling, disabling intake drivers
"""

import logging

from intake.cli.util import Subcommand
from intake.source.discovery import drivers

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

        list = sub_parser.add_parser("list", help="Show all intake drivers, whether enabled, disabled, " "or directly inserted into the registry")
        list.add_argument("-v", "--verbose", action="store_true", help="Show module path.")
        list.set_defaults(invoke=self._list)

        enable = sub_parser.add_parser("enable", help="Enable an intake driver.")
        enable.add_argument("name", type=str, help="Driver name")
        enable.add_argument("driver", type=str, default=None, nargs="?", help="Module path and class name, as in " "package.submodule.ClassName")
        enable.set_defaults(invoke=self._enable)

        disable = sub_parser.add_parser("disable", help="Disable one or more intake drivers.")
        disable.add_argument("names", type=str, help="Driver names", nargs="+")
        disable.set_defaults(invoke=self._disable)

    def invoke(self, args):
        self.parser.print_help()

    def _list(self, args):
        if drivers.do_scan:
            print("Package scan:")
            for k, v in drivers.scanned.items():
                print(f"{k:<30}{v.__module__}.{v.__name__}")
            print()

        print("Entrypoints:")
        eps = [ep for ep in drivers.from_entrypoints() if ep.name not in drivers.disabled()]
        if eps:
            for v in eps:
                print(f"{v.name:<30}{v.module_name}:{v.object_name}")
        else:
            print("<none>")
        print()

        print("From Config:")
        eps = [ep for ep in drivers.from_conf() if ep.name not in drivers.disabled()]
        if eps:
            for v in eps:
                if v.name not in drivers.disabled():
                    print(f"{v.name:<30}{v.module_name}:{v.object_name}")
        else:
            print("<none>")
        print()

        print("Disabled: ", drivers.disabled() or "<none>")

    def _enable(self, args):
        drivers.enable(args.name, args.driver)

    def _disable(self, args):
        for name in args.names:
            drivers.disable(name)
