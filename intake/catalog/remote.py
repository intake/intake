import operator

from ..plugins.base import DataSource
from . import serializer

import requests
from requests.compat import urljoin
import msgpack
import pandas
import numpy


class RemoteCatalog:
    def __init__(self, url):
        self._base_url = url + '/'
        self._info_url = urljoin(self._base_url, 'v1/info')
        self._source_url = urljoin(self._base_url, 'v1/source')

    def _get_info(self):
        response = requests.get(self._info_url)
        if response.status_code == 200:
            return msgpack.unpackb(response.content, encoding='utf-8')
        else:
            raise Exception('%s: status code %d' % (response.url, response.status_code))

    def list(self):
        info = self._get_info()
        return [s['name'] for s in info['sources']]

    def describe(self, entry_name):
        info = self._get_info()

        for source in info['sources']:
            if source['name'] == entry_name:
                return source
        else:
            raise Exception('unknown source %s' % entry_name)

    def get(self, entry_name, **user_parameters):
        entry = self.describe(entry_name)

        return RemoteDataSource(self._source_url, entry_name, container=entry['container'], user_parameters=user_parameters, description=entry['description'])


class RemoteDataSource(DataSource):
    def __init__(self, url, entry_name, container, user_parameters, description):
        self._init_args = dict(url=url, entry_name=entry_name, container=container, user_parameters=user_parameters, description=description)

        self._url = url
        self._entry_name = entry_name
        self._user_parameters = user_parameters

        self._source_id = None

        super().__init__(container=container, description=description)

    def _open_source(self):
        if self._source_id is None:
            payload = dict(action='open', name=self._entry_name, parameters=self._user_parameters)
            req = requests.post(self._url, data=msgpack.packb(payload, use_bin_type=True))
            if req.status_code == 200:
                response = msgpack.unpackb(req.content, encoding='utf-8')

                self.datashape = response['datashape']
                dtype_descr = response['dtype']
                if isinstance(dtype_descr, list):
                    # Reformat because NumPy needs list of tuples
                    dtype_descr = [tuple(x) for x in response['dtype']]
                self.dtype = numpy.dtype(dtype_descr)
                self.shape = tuple(response['shape'])
                self._source_id = response['source_id']

        return self._source_id

    def _get_chunks(self):
        source_id = self._open_source()

        accepted_formats = list(serializer.registry.keys())
        payload = dict(action='read', source_id=source_id, accepted_formats=accepted_formats)

        resp = None
        try:
            resp = requests.post(self._url, data=msgpack.packb(payload, use_bin_type=True), stream=True)
            if resp.status_code != 200:
                raise Exception('Error reading data')

            for msg in msgpack.Unpacker(resp.raw, encoding='utf-8'):
                format = msg['format']
                container = msg['container']
                chunk = serializer.registry[format].decode(msg['data'], container=container)
                yield chunk
        finally:
            if resp is not None:
                resp.close()
            
    def discover(self):
        self._open_source()
        return dict(datashape=self.datashape, dtype=self.dtype, shape=self.shape, npartitions=self.npartitions)

    def read(self):
        chunks = list(self._get_chunks())
        if self.container == 'dataframe':
            return pandas.concat(chunks)
        elif self.container == 'ndarray':
            return numpy.concatenate(chunks, axis=0)
        elif self.container == 'python':
            return reduce(operator.add, chunks)

    def read_chunked(self):
        for chunk in self._get_chunks():
            yield chunk

    def close(self):
        # FIXME: Need to tell server to delete source_id?
        self._source_id = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
