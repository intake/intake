import traceback
import sys

import zmq
import uuid

from .local import load_catalog

class ClientState:
    def __init__(self, ref_id, dataref):
        self.ref_id = ref_id
        self.dataref = dataref
        self.last_chunk_iter = None

    def read(self):
        return self.dataref.read()

    def read_chunks(self, chunksize):
        self.last_chunk_iter = self.dataref.read_chunks(chunksize)
        return next(self.last_chunk_iter)

    def next_chunk(self):
        return next(self.last_chunk_iter)


class Server:
    def __init__(self, bind_uri, local_catalog):
        self._local_catalog = local_catalog

        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind(bind_uri)
        self._open_datarefs = {}

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
                elif action == 'open_ref':
                    entry_name = message['name']
                    user_parameters = message['parameters']
                    ref = self._local_catalog.getref(entry_name, **user_parameters)
                    ref_id = str(uuid.uuid4())
                    self._open_datarefs[ref_id] = ClientState(ref_id, ref)
                    resp = dict(datashape=ref.datashape, dtype=ref.dtype, shape=ref.shape, container=ref.container, ref_id=ref_id)
                elif action == 'read':
                    state = self._open_datarefs[message['ref_id']]
                    resp = dict(data=state.read())
                elif action == 'read_chunks':
                    state = self._open_datarefs[message['ref_id']]
                    chunksize = message['chunksize']
                    try:
                        resp = dict(data=state.read_chunks(chunksize))
                    except StopIteration:
                        resp = dict(stop=True)
                elif action == 'next_chunk':
                    state = self._open_datarefs[message['ref_id']]
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
