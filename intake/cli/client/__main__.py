from __future__ import print_function

import sys
import os
import argparse
import shutil
import yaml

from intake import Catalog


def print_entry_info(catalog, name):
    info = catalog[name].describe()
    for key in sorted(info.keys()):
        print("[{}] {}={}".format(name, key, info[key]))


def listing(args):
    catalog = Catalog(args.uri)
    for entry in list(catalog):
        if args.full:
            print_entry_info(catalog, entry)
        else:
            print(entry)


def precache(args):
    catalog = Catalog(args.uri)
    for entry in list(catalog):
        s = catalog[entry]()
        if s.cache:
            print("Caching for entry %s" % entry)
            s.read()


def describe(args):
    catalog = Catalog(args.uri)
    print_entry_info(catalog, args.name)


def exists(args):
    catalog = Catalog(args.uri)
    print(args.name in catalog)


def get(args):
    catalog = Catalog(args.uri)
    with catalog[args.name].get() as f:
        print(f.read())


def discover(args):
    catalog = Catalog(args.uri)
    with catalog[args.name].get() as f:
        print(f.discover())


def example(args):
    print('Creating example catalog...')
    files = ['us_states.yml', 'states_1.csv', 'states_2.csv']
    for filename in files:
        if os.path.exists(filename):
            print('Cannot create example catalog in current directory.\n'
                  '%s already exists.' % filename)
            return 1

    src_dir = os.path.join(os.path.dirname(__file__), '..', 'sample')

    for filename in files:
        src_name = os.path.join(src_dir, filename)
        dest_name = filename
        dest_dir = os.path.dirname(filename)
        print('  Writing %s' % filename)
        if dest_dir != '' and not os.path.exists(dest_dir):
            os.mkdir(dest_dir)
        shutil.copyfile(src_name, dest_name)

    print('''\nTo load the catalog:
>>> import intake
>>> cat = intake.open_catalog('%s')
''' % files[0])


def conf_def(args):
    from intake.config import defaults
    print(yaml.dump(defaults, default_flow_style=False))


def conf_reset_save(args):
    from intake.config import reset_conf, save_conf
    reset_conf()
    save_conf()


def conf_get_key(args):
    from intake.config import conf
    if args.key:
        print(conf[args.key])
    else:
        print(yaml.dump(conf, default_flow_style=False))


def conf_show_info(args):
    from intake.config import cfile
    if 'INTAKE_CONF_DIR' in os.environ:
        print('INTAKE_CONF_DIR: ', os.environ['INTAKE_CONF_DIR'])
    if 'INTAKE_CONF_FILE' in os.environ:
        print('INTAKE_CONF_FILE: ', os.environ['INTAKE_CONF_FILE'])
    ex = "" if os.path.isfile(cfile()) else "(does not exist)"
    print('Using: ', cfile(), ex)


def cache_clear(args):
    from intake.source.cache import BaseCache
    c = BaseCache(None, None)
    if args.key is None:
        c.clear_all()
    else:
        c.clear_cache(args.key)


def cache_usage(args):
    from intake.config import conf
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(conf['cache_dir']):
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


def cache_list_files(args):
    from intake.source.cache import CacheMetadata
    md = CacheMetadata()
    print(yaml.dump(md[args.key], default_flow_style=False))


def cache_list_keys(args):
    from intake.source.cache import CacheMetadata
    md = CacheMetadata()
    print(yaml.dump(list(md), default_flow_style=False))


def cache_clear_all(args):
    from intake.source.cache import BaseCache
    if args.key is None:
        BaseCache(None, None).clear_all()
    else:
        BaseCache(None, None).clear_cache(args.key)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(description='Intake Catalog CLI', add_help=False)
    subparsers = parser.add_subparsers(help='sub-command help')

    list_parser = subparsers.add_parser('list', help='catalog listing')
    list_parser.add_argument('--full', action='store_true')
    list_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    list_parser.set_defaults(func=listing)

    describe_parser = subparsers.add_parser('describe', help='description for catalog entry')
    describe_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    describe_parser.add_argument('name', metavar='NAME', type=str, help='Catalog name')
    describe_parser.set_defaults(func=describe)

    exists_parser = subparsers.add_parser('exists', help='existence check for catalog entry')
    exists_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    exists_parser.add_argument('name', metavar='NAME', type=str, help='Catalog name')
    exists_parser.set_defaults(func=exists)

    precache_parser = subparsers.add_parser('precache', help='For each entry in a catalog which defined caching,'
                                                             ' populate the caching.')
    precache_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    precache_parser.set_defaults(func=precache)

    get_parser = subparsers.add_parser('get', help='get catalog entry')
    get_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    get_parser.add_argument('name', metavar='NAME', type=str, help='Catalog name')
    get_parser.set_defaults(func=get)

    discover_parser = subparsers.add_parser('discover', help='discover catalog entry')
    discover_parser.add_argument('uri', metavar='URI', type=str, help='Catalog URI')
    discover_parser.add_argument('name', metavar='NAME', type=str, help='Catalog name')
    discover_parser.set_defaults(func=discover)

    example_parser = subparsers.add_parser('example', help='create example catalog')
    example_parser.set_defaults(func=example)

    conf_parser = subparsers.add_parser('config', help='configuration functions')
    conf_sub = conf_parser.add_subparsers()
    conf_list = conf_sub.add_parser('list-defaults', help='Show all builtin defaults')
    conf_list.set_defaults(func=conf_def)
    conf_reset = conf_sub.add_parser('reset', help='Set config file to defaults')
    conf_reset.set_defaults(func=conf_reset_save)
    conf_info = conf_sub.add_parser('info', help='Show config settings')
    conf_info.set_defaults(func=conf_show_info)
    conf_get = conf_sub.add_parser('get', help='Get current config, specific key or all')
    conf_get.add_argument('key', type=str, help='Key in config dictionary',
                          nargs='?')
    conf_get.set_defaults(func=conf_get_key)

    cache_parser = subparsers.add_parser('cache', help='Locally cached files')
    cache_sub = cache_parser.add_subparsers()
    cache_list = cache_sub.add_parser('list-keys', help='List keys currently stored')
    cache_list.set_defaults(func=cache_list_keys)
    cache_files = cache_sub.add_parser('list-files', help='List files for a give key')
    cache_files.add_argument('key', type=str, help='Key to list files for')
    cache_files.set_defaults(func=cache_list_files)
    cache_rm = cache_sub.add_parser('clear')
    cache_rm.add_argument('key', type=str, help='Key to remove (all, if omitted)',
                          nargs='?')
    cache_rm.set_defaults(func=cache_clear_all)
    cache_du = cache_sub.add_parser('usage')
    cache_du.set_defaults(func=cache_usage)

    if not argv[1:]:
        parser.print_usage()
        sys.exit(1)

    args = parser.parse_args()
    if not hasattr(args, 'func') and '-h' not in argv:
        argv.append('-h')
        args = parser.parse_args()
    retcode = args.func(args)
    if retcode is None:
        retcode = 0
    return retcode


if __name__ == "__main__":
    sys.exit(main(sys.argv))
