# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import argparse
import logging
import signal
import sys

import tornado.ioloop
import tornado.web

from .server import IntakeServer

logger = logging.getLogger("intake")


def call_exit_on_sigterm(signal, frame):
    sys.exit(0)


def main(argv=None):
    from intake import open_catalog
    from intake.config import conf

    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(description="Intake Catalog Server")
    parser.add_argument("-p", "--port", type=int, default=conf["port"], help="port number for server to listen on")
    parser.add_argument("--list-entries", action="store_true", help="list catalog entries at startup")
    parser.add_argument("--sys-exit-on-sigterm", action="store_true", help="internal flag used during unit testing to ensure " ".coverage file is written")
    parser.add_argument("catalog_args", metavar="FILE", type=str, nargs="+", help="Name of catalog YAML file")
    parser.add_argument("--flatten", dest="flatten", action="store_true")
    parser.add_argument("--no-flatten", dest="flatten", action="store_false")
    parser.add_argument("--ttl", dest="ttl", type=int, default=60)
    parser.add_argument(
        "-a",
        "--address",
        type=str,
        default=conf.get("address", "localhost"),
        help="address to use as a host, defaults to the address " "in the configuration file, if provided otherwise localhost",
    )
    parser.set_defaults(flatten=True)
    args = parser.parse_args(argv[1:])

    if args.sys_exit_on_sigterm:
        signal.signal(signal.SIGTERM, call_exit_on_sigterm)

    logger.info("Creating catalog from:")
    for arg in args.catalog_args:
        logger.info("  - %s" % arg)

    catargs = args.catalog_args
    ttl = args.ttl

    if len(catargs) == 1:
        catalog = open_catalog(catargs[0], ttl=ttl)
        logger.info("catalog_args: %s" % catargs[0])
    else:
        catalog = open_catalog(catargs, flatten=args.flatten, ttl=ttl)
        logger.info("catalog_args: %s" % catargs)
    if args.list_entries:
        # This is not a good idea if the Catalog is huge.
        logger.info("Entries:" + ",".join(list(catalog)))

    logger.info("Listening on %s:%d" % (args.address, args.port))

    server = IntakeServer(catalog)
    app = server.make_app()
    server.start_periodic_functions(close_idle_after=3600.0)

    app.listen(args.port, address=args.address)
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        logger.critical("Exiting")
    except Exception as e:
        logger.critical("Exiting due to %s" % e)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
