from ..plugins.base import DataSource

import zmq


class RemoteCatalog:
    def __init__(self, uri):
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REQ)
        self._uri = uri
        self._socket.connect(uri)

    def list(self):
        self._socket.send_pyobj(dict(action='list'))
        response = self._socket.recv_pyobj()

        return response['entries']

    def describe(self, entry_name):
        self._socket.send_pyobj(dict(action='describe', name=entry_name))
        response = self._socket.recv_pyobj()

        return response 

    def get(self, entry_name, **user_parameters):
        return RemoteDataSource(self._uri, entry_name, user_parameters)


class RemoteDataSource(DataSource):
    def __init__(self, uri, entry_name, user_parameters):
        self._init_args = dict(uri=uri, entry_name=entry_name, user_parameters=user_parameters)

        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(uri)

        self._entry_name = entry_name
        self._user_parameters = user_parameters

        self._socket.send_pyobj(dict(action='open_source', name=entry_name, parameters=user_parameters))
        response = self._socket.recv_pyobj()

        if 'error' in response:
            raise Exception(response['error'])

        self._source_id = response['source_id']
        self.datashape = response['datashape']
        self.dtype = response['dtype']
        self.shape = response['shape']
        self.container = response['container']

    def read(self):
        self._socket.send_pyobj(dict(action='read', source_id=self._source_id))
        response = self._socket.recv_pyobj()

        if 'error' in response:
            raise Exception(response['error'])

        return response['data']

    def read_chunks(self, chunksize):
        self._socket.send_pyobj(dict(action='read_chunks', chunksize=chunksize, source_id=self._source_id))

        while True:
            response = self._socket.recv_pyobj()

            if 'error' in response:
                raise Exception(response['error'])
            if not response.get('stop', False):
                yield response['data']

                self._socket.send_pyobj(dict(action='next_chunk', source_id=self._source_id))
            else:
                break

    def close(self):
        pass # FIXME: What cleanup needs to happen here?

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
