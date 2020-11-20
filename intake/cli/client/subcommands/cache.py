#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import yaml
from intake.cli.util import Subcommand
import logging
log = logging.getLogger(__name__)


class Cache(Subcommand):
    """ Locally cached files

    """

    name = "cache"

    def initialize(self):
        sub_parser = self.parser.add_subparsers()

        cache_list = sub_parser.add_parser('list-keys', help='List keys currently stored')
        cache_list.set_defaults(invoke=self._list_keys)

        cache_files = sub_parser.add_parser('list-files', help='List files for a give key')
        cache_files.add_argument('key', type=str, help='Key to list files for')
        cache_files.set_defaults(invoke=self._list_files)

        cache_rm = sub_parser.add_parser('clear', help='Clear a key from the cache')
        cache_rm.add_argument('key', type=str, help='Key to remove (all, if omitted)', nargs='?')
        cache_rm.set_defaults(invoke=self._clear)

        cache_du = sub_parser.add_parser('usage', help='Print usage information')
        cache_du.set_defaults(invoke=self._usage)

    def invoke(self, args):
        self.parser.print_help()

    def _clear(self, args):
        from intake.source.cache import BaseCache
        if args.key is None:
            BaseCache(None, None).clear_all()
        else:
            BaseCache(None, None).clear_cache(args.key)

    def _list_keys(self, args):
        from intake.source.cache import CacheMetadata
        md = CacheMetadata()
        print(yaml.dump(list(md), default_flow_style=False))

    def _list_files(self, args):
        from intake.source.cache import CacheMetadata
        md = CacheMetadata()
        print(yaml.dump(md[args.key], default_flow_style=False))

    def _usage(self, args):
        from intake.config import conf
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(
                os.path.join(conf['cache_dir'], 'cache')):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
            # "human"
            # https://gist.github.com/cbwar/d2dfbc19b140bd599daccbe0fe925597
            if abs(total_size) < 1024.0:
                s = "%3.1f %s" % (total_size, unit)
                break
            total_size /= 1024.0
        print("%s: %s" % (conf['cache_dir'], s))
