# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
""" Provide a ``main`` function to run intake commands.

"""

import logging

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

# Standard library imports
import argparse

# Intake imports
from intake import __version__
from intake.cli.util import die, nice_join

# External imports


# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------


def main(description, subcommands, argv):
    """Execute an intake command.

    Args:
        description (str) :
            A description for this top-level command

        subcommands (seq[SubCommand]) :
            A list of subcommands to configure for argparse

        argv (seq[str]) :
            A list of command line arguments to process

    Returns:
        None

    """
    if len(argv) == 1:
        die("ERROR: Must specify subcommand, one of: %s" % nice_join(x.name for x in subcommands))

    parser = argparse.ArgumentParser(prog=argv[0], description=description, epilog="See '<command> --help' to read about a specific subcommand.")

    parser.add_argument("-v", "--version", action="version", version=__version__)

    subs = parser.add_subparsers(help="Sub-commands")

    for cls in subcommands:
        subparser = subs.add_parser(cls.name, help=cls.__doc__.strip())
        subcommand = cls(parser=subparser)
        subparser.set_defaults(invoke=subcommand.invoke)

    args = parser.parse_args(argv[1:])
    try:
        return args.invoke(args) or 0  # convert None to 0
    except Exception as e:
        die("ERROR: " + repr(e))
