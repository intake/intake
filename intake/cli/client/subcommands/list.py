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
from intake import open_catalog
from intake.cli.util import print_entry_info, Subcommand

#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

class List(Subcommand):
    ''' Show catalog listing

    '''

    name = "list"

    def initialize(self):
        self.parser.add_argument('--full', action='store_true')
        self.parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')

    def invoke(self, args):
        catalog = open_catalog(args.uri)
        for entry in list(catalog):
            if args.full:
                print_entry_info(catalog, entry)
            else:
                print(entry)
