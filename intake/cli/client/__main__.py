from __future__ import print_function

import sys
import os
import argparse
import shutil

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
>>> cat = intake.Catalog('%s')
''' % files[0])


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

    if not argv[1:]:
        parser.print_usage()
        sys.exit(1)

    args = parser.parse_args()
    retcode = args.func(args)
    if retcode is None:
        retcode = 0
    return retcode


if __name__ == "__main__":
    sys.exit(main(sys.argv))
