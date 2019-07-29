#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
'''

'''

import logging
log = logging.getLogger(__name__)

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

# Standard library imports

# External imports

# Intake imports
from intake import Catalog
from intake.cli.util import Subcommand

#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

class Get(Subcommand):
    ''' Get a catalog entry

    '''

    name = "get"

    def initialize(self):
        self.parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
        self.parser.add_argument('name', metavar='NAME', type=str, help='Catalog name')

    def invoke(self, args):
        catalog = Catalog(args.uri)
        with catalog[args.name].get() as f:
            print(f.read())
