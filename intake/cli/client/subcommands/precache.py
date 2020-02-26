#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from intake import Catalog
from intake.cli.util import Subcommand
import logging
log = logging.getLogger(__name__)


class Precache(Subcommand):
    """ Populate caching for catalog entries that define caching.

    """

    name = "precache"

    def initialize(self):
        self.parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')

    def invoke(self, args):
        catalog = Catalog(args.uri)
        for entry in list(catalog):
            s = catalog[entry]
            try:
                s = s()
                s.read()
                if s.cache:
                    print("Caching for entry %s" % entry)
            except Exception as e:
                print("Skipping {} due to {}".format(entry, e))
