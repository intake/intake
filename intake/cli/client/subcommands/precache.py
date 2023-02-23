# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
"""
"""

import logging

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

# Standard library imports

# External imports

# Intake imports
from intake import open_catalog
from intake.cli.util import Subcommand

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------


class Precache(Subcommand):
    """Populate caching for catalog entries that define caching."""

    name = "precache"

    def initialize(self):
        self.parser.add_argument("uri", metavar="URI", type=str, help="Catalog URI")

    def invoke(self, args):
        catalog = open_catalog(args.uri)
        for entry in list(catalog):
            try:
                s = catalog[entry]
                s.read()
                if s.cache:
                    print("Caching for entry %s" % entry)
            except Exception as e:
                print("Skipping {} due to {}".format(entry, e))
