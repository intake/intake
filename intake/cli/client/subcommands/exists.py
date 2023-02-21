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


class Exists(Subcommand):
    """Check for the existence of a catalog entry"""

    name = "exists"

    def initialize(self):
        self.parser.add_argument("uri", metavar="URI", type=str, help="Catalog URI")
        self.parser.add_argument("name", metavar="NAME", type=str, help="Catalog name")

    def invoke(self, args):
        catalog = open_catalog(args.uri)
        print(args.name in catalog)
