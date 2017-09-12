import traceback
import sys
import uuid

import tornado.web
import tornado.ioloop
import tornado.gen
import numpy
import msgpack

from . import serializer


def get_server_handlers(local_catalog):
    return [
        (r"/v1/info", ServerInfoHandler, dict(local_catalog=local_catalog)),
        (r"/v1/source", ServerSourceHandler, dict(local_catalog=local_catalog)),
    ]


class ServerInfoHandler(tornado.web.RequestHandler):
    def initialize(self, local_catalog):
        self.local_catalog = local_catalog

    def get(self):
        sources = []
        for source in self.local_catalog.list():
            info = self.local_catalog.describe(source)
            info['name'] = source
            sources.append(info)

        server_info = dict(version='0.0.1', sources=sources)
        self.write(msgpack.packb(server_info, use_bin_type=True))

OPEN_SOURCES = {}


class ServerSourceHandler(tornado.web.RequestHandler):
    def initialize(self, local_catalog):
        self._local_catalog = local_catalog

    @tornado.gen.coroutine
    def post(self):
        request = msgpack.unpackb(self.request.body, encoding='utf-8')
        action = request['action']

        if action == 'open':
            entry_name = request['name']
            user_parameters = request['parameters']
            source = self._local_catalog.get(entry_name, **user_parameters)
            source.discover()
            source_id = str(uuid.uuid4())
            OPEN_SOURCES[source_id] = ClientState(source_id, source)

            response = dict(datashape=source.datashape, dtype=numpy.dtype(source.dtype).descr, 
                shape=source.shape, container=source.container, 
                metadata=source.metadata, npartitions=source.npartitions,
                source_id=source_id)
            self.write(msgpack.packb(response))
            self.finish()
        elif action == 'read':
            source_id = request['source_id']
            state = OPEN_SOURCES[source_id]
            accepted_formats = request['accepted_formats']
            partition = request.get('partition', None)

            self._chunk_encoder = self._pick_encoder(accepted_formats, state.source.container)

            if partition is not None:
                self._iterator = iter([state.source.read_partition(partition)])
            else:
                self._iterator = state.source.read_chunked()
            self._container = state.source.container

            for chunk in self._iterator:
                data = self._chunk_encoder.encode(chunk, self._container)
                msg = dict(format=self._chunk_encoder.name, container=self._container, data=data)
                self.write(msgpack.packb(msg, use_bin_type=True))
                yield self.flush()

            self.finish()

        else:
            msg = '"%s" not a valid source action' % action
            raise tornado.web.HTTPError(status_code=400,
                log_message=msg,
                reason=msg)

    def _pick_encoder(self, accepted_formats, container):
        for f in accepted_formats:
            if f in serializer.registry:
                encoder = serializer.registry[f]
                return encoder

        msg = 'Unable to find compatible format'
        raise tornado.web.HTTPError(status_code=400,
                log_message=msg, reason=msg)

    def write_error(self, status_code, **kwargs):
        error_exception = kwargs.get('exc_info', None)
        if error_exception is not None:
            print(error_exception)
            msg = dict(error=error_exception[1].reason)
        else:
            msg = dict(error='unknown error')
        self.write(msgpack.packb(msg, use_bin_type=True))


class ClientState:
    def __init__(self, source_id, source):
        self.source_id = source_id
        self.source = source
