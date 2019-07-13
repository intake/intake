#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
''' Provide a ``main`` function to run intake commands.

'''

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
    ''' Abstract base class for subcommands

    Subclasses should define a class variable ``name`` that will be used as the
    subparser name, and a docstring, that will be used as the subparser help.
    After initialization, the parser for this comman will be avaialble as
    ``self.parser``.

    Subclasses must also implement:

    * an ``initialize(self)`` method that configures ``self.parser``

    * an ``invoke(self, args)`` method that accepts a set of argparse
      processed arguments as input.

    '''

    def __init__(self, parser):
        ''' Configure a parser for this command.

        '''
        self.parser = parser
        self.initialize()

    def initialize(self):
        ''' Implement in subclasses to configure self.parser with any arguments
        or additional sub-parsers.

        '''
        raise NotImplementedError("Subclasses must implement initialize()")

    def invoke(self, args):
        ''' Implement in subclasses to perform the actual work of the command

        '''
        raise NotImplementedError("Subclasses must implement invoke()")
