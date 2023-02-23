# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# ----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import logging

# Standard library imports
import sys

# Intake imports
from . import subcommands

# External imports

log = logging.getLogger("intake")

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------


def main(argv=None):
    """Execute the "intake" command line program."""
    from intake.cli.bootstrap import main as _main

    return _main("Intake Catalog CLI", subcommands.all, argv or sys.argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
