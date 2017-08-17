import traceback
import sys

import zmq
import uuid

from .local import load_catalog

class ClientState:
    def __init__(self, source_id, source):
        self.source_id = source_id
        self.source = source
        self.last_chunk_iter = None

    def read(self):
        return self.source.read()

    def read_chunks(self, chunksize):
        self.last_chunk_iter = self.source.read_chunks(chunksize)
        return next(self.last_chunk_iter)

    def next_chunk(self):
        return next(self.last_chunk_iter)


class Server:
    def __init__(self, bind_uri, local_catalog):
        self._local_catalog = local_catalog

        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind(bind_uri)
        self._open_sources = {}

    def run(self):
        try:

            while True:
                message = self._socket.recv_pyobj()
                action = message['action']
                print('action:', action)
                sys.stdout.flush()

                if action == 'list':
                    resp = dict(entries=self._local_catalog.list())
                elif action == 'describe':
                    entry_name = message['name']
                    resp = self._local_catalog.describe(entry_name)
                elif action == 'open_source':
                    entry_name = message['name']
                    user_parameters = message['parameters']
                    source = self._local_catalog.get(entry_name, **user_parameters)
                    source_id = str(uuid.uuid4())
                    self._open_sources[source_id] = ClientState(source_id, source)
                    resp = dict(datashape=source.datashape, dtype=source.dtype, shape=source.shape, container=source.container, source_id=source_id)
                elif action == 'read':
                    state = self._open_sources[message['source_id']]
                    resp = dict(data=state.read())
                elif action == 'read_chunks':
                    state = self._open_sources[message['source_id']]
                    chunksize = message['chunksize']
                    try:
                        resp = dict(data=state.read_chunks(chunksize))
                    except StopIteration:
                        resp = dict(stop=True)
                elif action == 'next_chunk':
                    state = self._open_sources[message['source_id']]
                    try:
                        resp = dict(data=state.next_chunk())
                    except StopIteration:
                        resp = dict(stop=True)
                else:
                    resp = dict(error='Unknown action %s' % action)

                self._socket.send_pyobj(resp)
        except Exception as e:
            traceback.print_exc()


if __name__ == '__main__':
    catalog_yaml = sys.argv[1]
    print('Loading %s' % catalog_yaml)

    catalog = load_catalog(catalog_yaml)

    print('Entries:', ','.join(catalog.list()))

    server = Server('tcp://*:5555', catalog)
    server.run()
