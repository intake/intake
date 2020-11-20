#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
import time
from uuid import uuid4

import itertools
import logging
import msgpack
import tornado.gen
import tornado.ioloop
import tornado.web

from intake.config import conf
from intake.container import serializer
from intake.utils import remake_instance
from intake import __version__
from intake.compat import unpack_kwargs, pack_kwargs
logger = logging.getLogger('intake')


class IntakeServer(object):
    """Main intake-server tornado application"""
    def __init__(self, catalog):
        self._catalog = catalog
        self._cache = SourceCache()
        self._periodic_callbacks = []
        auth = conf.get('auth', 'intake.auth.base.BaseAuth')
        logger.debug('auth: %s' % auth)
        self._auth = remake_instance(auth)

    def get_handlers(self):
        return [
            (r"/v1/info", ServerInfoHandler,
             dict(catalog=self._catalog, cache=self._cache, auth=self._auth)),
            (r"/v1/source", ServerSourceHandler,
             dict(catalog=self._catalog, cache=self._cache, auth=self._auth)),
        ]

    def make_app(self):
        handlers = self.get_handlers()
        return tornado.web.Application(handlers)

    def start_periodic_functions(self, close_idle_after=None,
                                 remove_idle_after=None):
        if len(self._periodic_callbacks) > 0:
            raise Exception('Periodic functions already started '
                            'for this server')

        # Disabling periodic timers with None makes testing easier
        if close_idle_after is not None:
            cache_closer = self._make_cache_closer(close_idle_after)
            self._periodic_callbacks.append(cache_closer)

        if remove_idle_after is not None:
            cache_remover = self._make_cache_remover(remove_idle_after)
            self._periodic_callbacks.append(cache_remover)

        for callback in self._periodic_callbacks:
            callback.start()

    def _make_cache_closer(self, idle_time):
        def cache_closer_callback():
            self._cache.close_idle(idle_time)
        return self._make_cache_callback(cache_closer_callback, idle_time)

    def _make_cache_remover(self, idle_time):
        def cache_remover_callback():
            self._cache.remove_idle(idle_time)
        return self._make_cache_callback(cache_remover_callback, idle_time)

    def _make_cache_callback(self, callback, idle_time):
        # Check ever 1/10 of the idle_time
        interval_ms = (idle_time / 10.0) * 1000
        callback = tornado.ioloop.PeriodicCallback(callback, interval_ms)
        return callback


class ServerInfoHandler(tornado.web.RequestHandler):
    """Basic info about the server"""
    def initialize(self, cache, catalog, auth):
        self.cache = cache
        self.catalog = catalog
        self.auth = auth

    def get(self):
        head = self.request.headers
        page_size = self.get_argument('page_size', None)
        page_offset = self.get_argument('page_offset', 0)
        if self.auth.allow_connect(head):
            if 'source-id' in head:
                cat = self.cache.get(head['source-id'])
            else:
                cat = self.catalog
            sources = []
            if page_size is None:
                # Return all the results in one page. This is important for
                # back-compat with clients that predate pagination. It may
                # also be useful to keep things simple for clients that do not
                # need pagination.
                start = stop = None
            else:
                start = int(page_offset)
                stop = int(page_offset) + int(page_size)
            page = itertools.islice(cat.items(), start, stop)
            for name, source in page:
                if self.auth.allow_access(head, source, self.catalog):
                    info = source.describe().copy()
                    modified_args = info['args'].copy()
                    info['name'] = name
                    for k, v in info['args'].items():
                        try:
                            msgpack.packb(v, **pack_kwargs)
                        except TypeError:
                            modified_args[k] = 'UNSERIALIZABLE_VALUE'
                    info['args'] = modified_args
                    sources.append(info)
            try:
                length = len(cat)
            except TypeError:
                length = sum(1 for entry in cat)
            server_info = dict(version=__version__, sources=sources,
                               length=length,
                               metadata=cat.metadata)
        else:
            msg = 'Access forbidden'
            raise tornado.web.HTTPError(status_code=403, log_message=msg,
                                        reason=msg)
        self.write(msgpack.packb(server_info, **pack_kwargs))


class SourceCache(object):
    """Stores DataSources requested by some user"""
    def __init__(self):
        self._sources = {}

    def add(self, source, source_id=None):
        if source_id is None:
            source_id = str(uuid4())
        now = time.time()
        self._sources[source_id] = dict(source=source, open_time=now,
                                        last_time=now)
        logger.debug('Adding %s to cache, uuid %s' % (source, source_id))
        return source_id

    def get(self, uuid):
        record = self._sources[uuid]
        record['last_time'] = time.time()
        return record['source']

    def peek(self, uuid):
        """Get the source but do not change the last access time"""
        return self._sources[uuid]['source']

    def touch(self, uuid):
        record = self._sources[uuid]
        record['last_time'] = time.time()

    def close_idle(self, idle_secs):
        threshold = time.time() - idle_secs

        for record in self._sources.values():
            if record['last_time'] < threshold:
                record['source'].close()

    def remove_idle(self, idle_secs):
        threshold = time.time() - idle_secs

        # Make a copy of the items so we can mutate the dictionary
        for uuid, record in list(self._sources.items()):
            if record['last_time'] < threshold:
                logger.debug('Removing source %s from cache' % uuid)
                del self._sources[uuid]


class ServerSourceHandler(tornado.web.RequestHandler):
    """Open or stream data source

    The requests "action" field (open|read) specified what the request wants
    to do. Open caches the source and created an ID for it, read uses that
    ID to reference the source and read a partition.
    """
    def initialize(self, catalog, cache, auth):
        self._catalog = catalog
        self._cache = cache
        self.auth = auth

    def get(self):
        """
        Access one source's info.

        This is for direct access to an entry by name for random access, which
        is useful to the client when the whole catalog has not first been
        listed and pulled locally (e.g., in the case of pagination).
        """
        head = self.request.headers
        name = self.get_argument('name')
        if self.auth.allow_connect(head):
            if 'source-id' in head:
                cat = self._cache.get(head['source-id'])
            else:
                cat = self._catalog
            try:
                source = cat[name]
            except KeyError:
                msg = 'No such entry'
                raise tornado.web.HTTPError(status_code=404, log_message=msg,
                                            reason=msg)
            if self.auth.allow_access(head, source, self._catalog):
                info = source.describe().copy()
                info['name'] = name

                source_info = dict(source=info)
                try:
                    out = msgpack.packb(source_info, **pack_kwargs)
                except TypeError:
                    info['direct_access'] = 'forbid'
                    modified_args = source_info['source']['args'].copy()
                    for k, v in source_info['source']['args'].items():
                        try:
                            msgpack.packb(v, **pack_kwargs)
                        except TypeError:
                            modified_args[k] = 'UNSERIALIZABLE_VALUE'
                    source_info['source']['args'] = modified_args
                    out = msgpack.packb(source_info, **pack_kwargs)
                self.write(out)
                return

        msg = 'Access forbidden'
        raise tornado.web.HTTPError(status_code=403, log_message=msg,
                                    reason=msg)

    @tornado.gen.coroutine
    def post(self):
        request = msgpack.unpackb(self.request.body, **unpack_kwargs)
        action = request['action']
        head = self.request.headers
        logger.debug('Source POST: %s' % request)
        
        if action == 'search':
            if 'source-id' in head:
                cat = self._cache.get(head['source-id'])
            else:
                cat = self._catalog
            query = request['query']
            # Construct a cache key from the source_id of the Catalog being
            # searched and the query itself.
            query_source_id = '-'.join((head.get('source-id', 'root'),
                                        str(query)))
            try:
                cat = self._cache.get(query_source_id)
            except KeyError:
                try:
                    args, kwargs = query
                    results_cat = cat.search(*args, **kwargs)
                except Exception as err:
                    logger.exception("Search query %r on Catalog %r failed",
                                     query, cat)
                    raise tornado.web.HTTPError(
                        status_code=400,
                        log_message="Search query failed",
                        reason=str(err))
                self._cache.add(results_cat, source_id=query_source_id)
            response = {'source_id': query_source_id}
            self.write(msgpack.packb(response, **pack_kwargs))
            self.finish()
        elif action == 'open':
            if 'source-id' in head:
                cat = self._cache.get(head['source-id'])
            else:
                cat = self._catalog
            entry_name = request['name']
            try:
                entry = cat[entry_name]
            except KeyError:
                msg = "Catalog has no entry {!r}".format(entry_name)
                raise tornado.web.HTTPError(status_code=404, log_message=msg,
                                            reason=msg)
            if not self.auth.allow_access(head, entry, cat):
                msg = 'Access forbidden'
                raise tornado.web.HTTPError(status_code=403, log_message=msg,
                                            reason=msg)
            user_parameters = request.get('parameters', {})
            client_plugins = request.get('available_plugins', [])

            # Can the client directly access the data themselves?
            open_desc = entry.describe()
            direct_access = open_desc['direct_access']
            plugin_name = open_desc['plugin']
            if isinstance(plugin_name, list):
                for pl in plugin_name:
                    if pl in client_plugins:
                        plugin_name = pl
                        break
            client_has_plugin = plugin_name in client_plugins

            if direct_access == 'forbid' or \
                    (direct_access == 'allow' and not client_has_plugin):
                logger.debug("Opening entry %s" % entry)
                source = entry.configure_new(**user_parameters)
                try:
                    source.on_server = True
                    source.discover()
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    raise tornado.web.HTTPError(status_code=400,
                                                log_message="Discover failed",
                                                reason=str(e))
                source_id = self._cache.add(source)
                logger.debug('Container: %s, ID: %s' % (source.container,
                                                        source_id))
                response = dict(source._schema or {})
                response.update(dict(container=source.container,
                                     source_id=source_id,
                                     metadata=source.metadata))
                self.write(msgpack.packb(response, **pack_kwargs))
                self.finish()
            elif direct_access == 'force' and not client_has_plugin:
                msg = 'client must have plugin "%s" to access source "%s"' \
                      '' % (plugin_name, entry_name)
                raise tornado.web.HTTPError(status_code=400, log_message=msg,
                                            reason=msg)
            else:
                # If we get here, the client can access the source directly
                # some server-side args need to be parsed
                response = open_desc
                user_parameters['plugin'] = plugin_name
                response['args'] = (entry._entry._create_open_args(user_parameters)[1])
                self.write(msgpack.packb(response, **pack_kwargs))
                self.finish()

        elif action == 'read':
            source_id = request['source_id']
            source = self._cache.get(source_id)
            accepted_formats = request['accepted_formats']
            accepted_compression = request.get('accepted_compression', ['none'])
            partition = request.get('partition', None)

            chunk_encoder = self._pick_encoder(accepted_formats,
                                               accepted_compression,
                                               source.container)

            logger.debug("Read partition %s" % partition)
            if partition is not None:
                chunk = source.read_partition(partition)
            else:
                assert source.npartitions < 2
                chunk = source.read()

            data = chunk_encoder.encode(chunk, source.container)
            msg = dict(format=chunk_encoder.format_name,
                       compression=chunk_encoder.compressor_name,
                       container=source.container, data=data)
            self.write(msgpack.packb(msg, **pack_kwargs))
            self.flush()
            self._cache.touch(source_id)  # keep source alive

            self.finish()

        else:
            msg = '"%s" not a valid source action' % action
            raise tornado.web.HTTPError(status_code=400, log_message=msg,
                                        reason=msg)

    def _pick_encoder(self, accepted_formats, accepted_compression, container):
        format_encoder = None

        for f in accepted_formats:
            if f in serializer.format_registry:
                format_encoder = serializer.format_registry[f]

        if format_encoder is None:
            msg = 'Unable to find compatible format'
            raise tornado.web.HTTPError(status_code=400, log_message=msg,
                                        reason=msg)

        compressor = serializer.NoneCompressor()  # Default
        for f in accepted_compression:
            if f in serializer.compression_registry:
                compressor = serializer.compression_registry[f]

        return serializer.ComboSerializer(format_encoder, compressor)
