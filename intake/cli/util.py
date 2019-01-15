#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
''' Provide a ``main`` function to run intake commands.

'''

from __future__ import print_function

import logging
log = logging.getLogger(__name__)

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

# Standard library imports
import sys

# External imports

# Intake imports


#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

def die(message, status=1):
    ''' Print an error message and exit.
    This function will call ``sys.exit`` with the given ``status`` and the
    process will terminate.

    Args:
        message (str) : error message to print

        status (int) : the exit status to pass to ``sys.exit``

    '''
    print(message, file=sys.stderr)
    sys.exit(status)

def nice_join(seq, sep=", ", conjunction="or"):
    ''' Join together sequences of strings into English-friendly phrases using
    a conjunction when appropriate.

    Args:
        seq (seq[str]) : a sequence of strings to nicely join

        sep (str, optional) : a sequence delimiter to use (default: ", ")

        conjunction (str or None, optional) : a conjunction to use for the last
            two items, or None to reproduce basic join behavior (default: "or")

    Returns:
        a joined string

    Examples:
        >>> nice_join(["a", "b", "c"])
        'a, b or c'

    '''
    seq = [str(x) for x in seq]

    if len(seq) <= 1 or conjunction is None:
        return sep.join(seq)
    else:
        return "%s %s %s" % (sep.join(seq[:-1]), conjunction, seq[-1])

def print_entry_info(catalog, name):
    '''

    '''
    info = catalog[name].describe()
    for key in sorted(info.keys()):
        print("[{}] {}={}".format(name, key, info[key]))

class Subcommand(object):
    '''

    '''

    def __init__(self, parser):
        self.parser = parser
        self.initialize()

    def initialize(self, args):
        raise NotImplementedError("Subclasses must implement initialize()")

    def invoke(self, args):
        raise NotImplementedError("Subclasses must implement invoke()")
