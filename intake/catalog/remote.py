import operator
import sys

from requests.compat import urljoin
import msgpack
import numpy
import pandas
import requests

from ..source.base import DataSource
from ..source import registry as plugin_registry
from . import serializer
from . import dask_util


class RemoteCatalog(object):
    def __init__(self, url):
        self._base_url = url + '/'
        self._info_url = urljoin(self._base_url, 'v1/info')
        self._source_url = urljoin(self._base_url, 'v1/source')

    def _get_info(self):
        response = requests.get(self._info_url)
        if response.status_code == 200:
            return msgpack.unpackb(response.content, encoding=sys.getdefaultencoding())
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

        self._real_source = None
        self.direct = None

        super(RemoteDataSource, self).__init__(container=container, description=description)

    def _open_source(self):
        if self._real_source is None:
            payload = dict(action='open',
                           name=self._entry_name,
                           parameters=self._user_parameters,
                           available_plugins=list(plugin_registry.keys()))
            req = requests.post(self._url, data=msgpack.packb(payload, use_bin_type=True))
            if req.status_code == 200:
                response = msgpack.unpackb(req.content, encoding=sys.getdefaultencoding())

                if 'plugin' in response:
                    # Direct access
                    self._open_direct(response)
                    self.direct = True
                else:
                    # Proxied access
                    self._real_source = RemoteDataSourceProxied(**self._init_args)
                    self._real_source._parse_open_response(response)
                    self.direct = False

            else:
                raise Exception('Server error: %d' % req.status_code)

    def _open_direct(self, response):
        plugin = plugin_registry[response['plugin']]
        source = plugin.open(**response['args'])
        source.description = response['description']
        self._real_source = source

    def _copy_attributes(self):
        for attr in ['datashape', 'dtype', 'shape', 'npartitions', 'metadata']:
            setattr(self, attr, getattr(self._real_source, attr))

    def discover(self):
        self._open_source()
        ret = self._real_source.discover()
        self._copy_attributes()
        return ret

    def read(self):
        self._open_source()
        return self._real_source.read()

    def read_chunked(self):
        self._open_source()
        for chunk in self._real_source.read_chunked():
            yield chunk

    def read_partition(self, i):
        self._open_source()
        return self._real_source.read_partition(i)

    def to_dask(self):
        self._open_source()
        return self._real_source.to_dask()

    def close(self):
        if self._real_source is not None:
            self._real_source.close()
            self._real_source = None

    def __getstate__(self):
        return self._init_args, self._real_source

    def __setstate__(self, state):
        init_args, real_source = state
        self.__init__(**init_args)
        self._real_source = real_source
        self.direct = not isinstance(self._real_source, RemoteDataSourceProxied)
        if self._real_source is not None:
            self._copy_attributes()


class RemoteDataSourceProxied(DataSource):
    def __init__(self, url, entry_name, container, user_parameters, description):
        self._init_args = dict(url=url, entry_name=entry_name, container=container, user_parameters=user_parameters, description=description)

        self._url = url
        self._entry_name = entry_name
        self._user_parameters = user_parameters

        self._source_id = None

        super(RemoteDataSourceProxied, self).__init__(container=container, description=description)

    def _open_source(self):
        if self._source_id is None:
            payload = dict(action='open', name=self._entry_name, parameters=self._user_parameters)
            req = requests.post(self._url, data=msgpack.packb(payload, use_bin_type=True))
            if req.status_code == 200:
                response = msgpack.unpackb(req.content, encoding=sys.getdefaultencoding())
                self._parse_open_response(response)

        return self._source_id

    def _parse_open_response(self, response):
        self.datashape = response['datashape']
        dtype_descr = response['dtype']
        if isinstance(dtype_descr, list):
            # Reformat because NumPy needs list of tuples
            dtype_descr = [tuple(x) for x in response['dtype']]
        self.dtype = numpy.dtype(dtype_descr)
        self.shape = tuple(response['shape'])
        self.npartitions = response['npartitions']
        self.metadata = response['metadata']
        self._source_id = response['source_id']

    def _get_chunks(self, partition=None):
        source_id = self._open_source()

        accepted_formats = list(serializer.format_registry.keys())
        accepted_compression = list(serializer.compression_registry.keys())
        payload = dict(action='read',
                       source_id=source_id,
                       accepted_formats=accepted_formats,
                       accepted_compression=accepted_compression)

        if partition is not None:
            payload['partition'] = partition

        resp = None
        try:
            resp = requests.post(self._url, data=msgpack.packb(payload, use_bin_type=True), stream=True)
            if resp.status_code != 200:
                raise Exception('Error reading data')

            for msg in msgpack.Unpacker(resp.raw, encoding=sys.getdefaultencoding()):
                format = msg['format']
                compression = msg['compression']
                container = msg['container']

                compressor = serializer.compression_registry[compression]
                encoder = serializer.format_registry[format]
                chunk = encoder.decode(compressor.decompress(msg['data']), container=container)
                yield chunk
        finally:
            if resp is not None:
                resp.close()

    def _read(self, partition=None):
        chunks = list(self._get_chunks(partition=partition))
        if self.container == 'dataframe':
            return pandas.concat(chunks)
        elif self.container == 'ndarray':
            return numpy.concatenate(chunks, axis=0)
        elif self.container == 'python':
            return reduce(operator.add, chunks)

    def discover(self):
        self._open_source()
        return dict(datashape=self.datashape, dtype=self.dtype, shape=self.shape, npartitions=self.npartitions)

    def read(self):
        return self._read()

    def read_chunked(self):
        for chunk in self._get_chunks():
            yield chunk

    def read_partition(self, i):
        return self._read(partition=i)

    def to_dask(self):
        self._open_source()
        return dask_util.to_dask(self)

    def close(self):
        # Server removes source after timeout
        self._source_id = None

    def __getstate__(self):
        return dict(init_args=self._init_args, source_id=self._source_id)

    def __setstate__(self, state):
        self.__init__(**state['init_args'])
        self._source_id = state['source_id']
