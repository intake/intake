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
import os

# External imports
import yaml

# Intake imports
from intake.cli.util import Subcommand
#-----------------------------------------------------------------------------
# API
#-----------------------------------------------------------------------------

class Config(Subcommand):
    ''' Configuration functions

    '''

    name = "config"

    def initialize(self):
        sub_parser = self.parser.add_subparsers()

        list = sub_parser.add_parser('list-defaults', help='Show all builtin defaults')
        list.set_defaults(invoke=self._list_defaults)

        conf_reset = sub_parser.add_parser('reset', help='Set config file to defaults')
        conf_reset.set_defaults(invoke=self._reset)

        conf_info = sub_parser.add_parser('info', help='Show config settings')
        conf_info.set_defaults(invoke=self._info)

        conf_get = sub_parser.add_parser('get', help='Get current config, specific key or all')
        conf_get.add_argument('key', type=str, help='Key in config dictionary', nargs='?')
        conf_get.set_defaults(invoke=self._get)

    def invoke(self, args):
        self.parser.print_help()

    def _get(self, args):
            from intake.config import conf
            if args.key:
                print(conf[args.key])
            else:
                print(yaml.dump(conf, default_flow_style=False))

    def _info(self, args):
        from intake.config import cfile
        if 'INTAKE_CONF_DIR' in os.environ:
            print('INTAKE_CONF_DIR: ', os.environ['INTAKE_CONF_DIR'])
        if 'INTAKE_CONF_FILE' in os.environ:
            print('INTAKE_CONF_FILE: ', os.environ['INTAKE_CONF_FILE'])
        ex = "" if os.path.isfile(cfile()) else "(does not exist)"
        print('Using: ', cfile(), ex)

    def _list_defaults(self, args):
        from intake.config import defaults
        print(yaml.dump(defaults, default_flow_style=False))

    def _reset(self, args):
        from intake.config import reset_conf, save_conf
        reset_conf()
        save_conf()
