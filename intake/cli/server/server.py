from __future__ import print_function

import time
from uuid import uuid4

import msgpack
import pickle
import tornado.gen
import tornado.ioloop
import tornado.web

from .browser import get_browser_handlers
from .config import conf
from intake.catalog import serializer
from intake.auth import get_auth_class
from intake import __version__


class IntakeServer(object):
    def __init__(self, catalog):
        self._catalog = catalog
        self._cache = SourceCache()
        self._periodic_callbacks = []
        auth = conf.get('auth', 'intake.auth.base.BaseAuth')
        self._auth = get_auth_class(auth['class'], *auth.get('args', tuple()),
                                    **auth.get('kwargs', {}))

    def get_handlers(self):
        return [
            (r"/v1/info", ServerInfoHandler,
             dict(catalog=self._catalog, auth=self._auth)),
            (r"/v1/source", ServerSourceHandler,
             dict(catalog=self._catalog, cache=self._cache, auth=self._auth)),
        ]

    def make_app(self):
        handlers = get_browser_handlers(self._catalog) + self.get_handlers()
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
    def initialize(self, catalog, auth):
        self.catalog = catalog
        self.auth = auth

    def get(self):
        head = self.request.headers
        if self.auth.allow_connect(head):
            sources = []
            for _, name, source in self.catalog.walk():
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
    def __init__(self):
        self._sources = {}

    def add(self, source):
        source_id = str(uuid4())
        now = time.time()
        self._sources[source_id] = dict(source=source, open_time=now,
                                        last_time=now)
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
                del self._sources[uuid]


class ServerSourceHandler(tornado.web.RequestHandler):
    def initialize(self, catalog, cache, auth):
        self._catalog = catalog
        self._cache = cache
        self.auth = auth

    @tornado.gen.coroutine
    def post(self):
        request = msgpack.unpackb(self.request.body, encoding='utf-8')
        action = request['action']
        head = self.request.headers

        if action == 'open':
            entry_name = request['name']
            entry = self._catalog[entry_name]
            if not self.auth.allow_access(head, entry, self._catalog):
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
                source = entry.get(**user_parameters)
                source.discover()
                source_id = self._cache.add(source)

                response = dict(
                    datashape=source.datashape,
                    dtype=pickle.dumps(source.dtype, 2),
                    shape=source.shape, container=source.container,
                    metadata=source.metadata, npartitions=source.npartitions,
                    source_id=source_id)
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

            self._chunk_encoder = self._pick_encoder(accepted_formats,
                                                     accepted_compression,
                                                     source.container)

            if partition is not None:
                self._iterator = iter([source.read_partition(partition)])
            else:
                self._iterator = source.read_chunked()
            self._container = source.container

            for chunk in self._iterator:
                data = self._chunk_encoder.encode(chunk, self._container)
                msg = dict(format=self._chunk_encoder.format_name,
                           compression=self._chunk_encoder.compressor_name,
                           container=self._container, data=data)
                self.write(msgpack.packb(msg, use_bin_type=True))
                yield self.flush()
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
            print(error_exception)
            msg = dict(error=str(error_exception[1]))
        else:
            msg = dict(error='unknown error')
        self.write(msgpack.packb(msg, use_bin_type=True))
