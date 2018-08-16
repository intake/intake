from __future__ import print_function

import time
from uuid import uuid4

import logging
import msgpack
import pickle
import tornado.gen
import tornado.ioloop
import tornado.web

from intake.config import conf
from intake.container import serializer
from intake.auth import get_auth_class
from intake import __version__
logger = logging.getLogger('intake')


class IntakeServer(object):
    """Main intake-server tornado application"""
    def __init__(self, catalog):
        self._catalog = catalog
        self._cache = SourceCache()
        self._periodic_callbacks = []
        auth = conf.get('auth', 'intake.auth.base.BaseAuth')
        logger.debug('auth: %s' % auth)
        self._auth = get_auth_class(auth['class'], *auth.get('args', tuple()),
                                    **auth.get('kwargs', {}))

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
        if self.auth.allow_connect(head):
            if 'source_id' in head:
                cat = self.cache.get(head['source_id'])
            else:
                cat = self.catalog
            sources = []
            for name, source in cat.walk(depth=1).items():
                if self.auth.allow_access(head, source, self.catalog):
                    info = source.describe()
                    info['name'] = name
                    sources.append(info)

            server_info = dict(version=__version__, sources=sources,
                               metadata=self.catalog.metadata)
        else:
            msg = 'Access forbidden'
            raise tornado.web.HTTPError(status_code=403, log_message=msg,
                                        reason=msg)
        self.write(msgpack.packb(server_info, use_bin_type=True))


class SourceCache(object):
    """Stores DataSources requested by some user"""
    def __init__(self):
        self._sources = {}

    def add(self, source):
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

    @tornado.gen.coroutine
    def post(self):
        request = msgpack.unpackb(self.request.body, encoding='utf-8')
        action = request['action']
        head = self.request.headers
        logger.debug('Source POST: %s' % request)

        if action == 'open':
            if 'source_id' in head:
                cat = self._cache.get(head['source_id'])
            else:
                cat = self._catalog
            entry_name = request['name']
            entry = cat[entry_name]
            if not self.auth.allow_access(head, entry, cat):
                msg = 'Access forbidden'
                raise tornado.web.HTTPError(status_code=403, log_message=msg,
                                            reason=msg)
            user_parameters = request['parameters']
            client_plugins = request.get('available_plugins', [])

            # Can the client directly access the data themselves?
            open_desc = entry.describe_open(**user_parameters)
            direct_access = open_desc['direct_access']
            plugin_name = open_desc['plugin']
            client_has_plugin = plugin_name in client_plugins

            if direct_access == 'forbid' or \
                    (direct_access == 'allow' and not client_has_plugin):
                logger.debug("Opening entry %s" % entry)
                source = entry.get(**user_parameters)
                try:
                    source.discover()
                except Exception as e:
                    raise tornado.web.HTTPError(status_code=400,
                                                log_message="Discover failed",
                                                reason=str(e))
                source_id = self._cache.add(source)
                logger.debug('Container: %s, ID: %s' % (source.container,
                                                        source_id))
                response = dict(source._schema)
                response.update(dict(container=source.container,
                                     source_id=source_id,
                                     metadata=source.metadata))
                self.write(msgpack.packb(response, use_bin_type=True))
                self.finish()
            elif direct_access == 'force' and not client_has_plugin:
                msg = 'client must have plugin "%s" to access source "%s"' \
                      '' % (plugin_name, entry_name)
                raise tornado.web.HTTPError(status_code=400, log_message=msg,
                                            reason=msg)
            else:
                # If we get here, the client can access the source directly
                response = dict(
                    plugin=plugin_name, args=open_desc['args'],
                    description=open_desc['description'])
                self.write(msgpack.packb(response, use_bin_type=True))
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
            self.write(msgpack.packb(msg, use_bin_type=True))
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

    def write_error(self, status_code, **kwargs):
        error_exception = kwargs.get('exc_info', None)
        if error_exception is not None:
            msg = dict(error=str(error_exception[1]))
        else:
            msg = dict(error='unknown error')
        self.write(msgpack.packb(msg, use_bin_type=True))
