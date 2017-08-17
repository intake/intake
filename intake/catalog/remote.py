import zmq

class RemoteCatalog:
    def __init__(self, uri):
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(uri)

    def list(self):
        self._socket.send_pyobj(dict(action='list'))
        response = self._socket.recv_pyobj()

        return response['entries']

    def describe(self, entry_name):
        self._socket.send_pyobj(dict(action='describe', name=entry_name))
        response = self._socket.recv_pyobj()

        return response 

    def getref(self, entry_name, **user_parameters):
        return RemoteDataRef(self._socket, entry_name, user_parameters)

class RemoteDataRef:
    def __init__(self, socket, entry_name, user_parameters):
        self._socket = socket
        self._entry_name = entry_name
        self._user_parameters = user_parameters

        self._socket.send_pyobj(dict(action='open_ref', name=entry_name, parameters=user_parameters))
        response = self._socket.recv_pyobj()

        if 'error' in response:
            raise Exception(response['error'])

        self._ref_id = response['ref_id']
        self.datashape = response['datashape']
        self.dtype = response['dtype']
        self.shape = response['shape']
        self.container = response['container']

    def read(self):
        self._socket.send_pyobj(dict(action='read', ref_id=self._ref_id))
        response = self._socket.recv_pyobj()

        if 'error' in response:
            raise Exception(response['error'])

        return response['data']

    def read_chunks(self, chunksize):
        self._socket.send_pyobj(dict(action='read_chunks', chunksize=chunksize, ref_id=self._ref_id))

        while True:
            response = self._socket.recv_pyobj()

            if 'error' in response:
                raise Exception(response['error'])
            if not response.get('stop', False):
                yield response['data']

                self._socket.send_pyobj(dict(action='next_chunk', ref_id=self._ref_id))
            else:
                break
