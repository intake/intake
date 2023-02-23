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

import os
import shutil

# Standard library imports
from os.path import dirname, exists, join

# Intake imports
from intake.cli.util import Subcommand

# External imports


# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------


class Example(Subcommand):
    """Create example catalog"""

    name = "example"

    def initialize(self):
        pass

    def invoke(self, args):
        print("Creating example catalog...")
        files = ["us_states.yml", "states_1.csv", "states_2.csv"]
        for filename in files:
            if exists(filename):
                print("Cannot create example catalog in current directory.\n" "%s already exists." % filename)
                return 1

        src_dir = join(dirname(__file__), "..", "..", "sample")

        for filename in files:
            src_name = join(src_dir, filename)
            dest_name = filename
            dest_dir = dirname(filename)
            print("  Writing %s" % filename)
            if dest_dir != "" and not exists(dest_dir):
                os.mkdir(dest_dir)
            shutil.copyfile(src_name, dest_name)

        print(
            """\nTo load the catalog:
    >>> import intake
    >>> cat = intake.open_catalog('%s')
    """
            % files[0]
        )
