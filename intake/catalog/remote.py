import msgpack
import numpy
import pickle
import requests
import six

from . import dask_util
from . import serializer
from ..container import get_container_klass
from ..source import registry as plugin_registry
from ..source.base import DataSource
from .entry import CatalogEntry
from .utils import expand_defaults, coerce


class RemoteCatalogEntry(CatalogEntry):
    def __init__(self, url, auth, *args, **kwargs):
        self.url = url
        self.auth = auth
        self.args = args
        self.kwargs = kwargs
        getenv = kwargs.pop('getenv', True)
        getshell = kwargs.pop('getshell', True)
        self.http_args = kwargs.pop('http_args', {}).copy()
        if 'headers' not in self.http_args:
            self.http_args['headers'] = {}
        super(RemoteCatalogEntry, self).__init__(getenv=getenv,
                                                 getshell=getshell)

    def describe(self):
        return self.kwargs

    def get(self, **user_parameters):
        for par in self.kwargs['user_parameters']:
            if par['name'] not in user_parameters:
                default = par['default']
                if isinstance(default, six.string_types):
                    default = coerce(par['type'], expand_defaults(
                        par['default'], True, self.getenv, self.getshell))
                user_parameters[par['name']] = default
        entry = self.kwargs

        # We must freeze the auth headers at this point since RemoteDataSource may be serialized
        http_args = self.http_args.copy()
        http_args['headers'] = self.http_args['headers'].copy()
        http_args['headers'].update(self.auth.get_headers())
        return RemoteDataSource(
            self.url, entry['name'], container=entry['container'],
            user_parameters=user_parameters, description=entry['description'],
            http_args=self.http_args
            )


class RemoteDataSource(DataSource):
    def __init__(self, url, entry_name, container, user_parameters,
                 description, http_args):
        self._init_args = dict(
            url=url, entry_name=entry_name, container=container,
            user_parameters=user_parameters, description=description,
            http_args=http_args)

        self._url = url
        self._entry_name = entry_name
        self._user_parameters = user_parameters
        self._http_args = http_args

        self._real_source = None
        self.direct = None

        super(RemoteDataSource, self).__init__(container=container,
                                               description=description)

    def _open_source(self):
        if self._real_source is None:
            payload = dict(action='open',
                           name=self._entry_name,
                           parameters=self._user_parameters,
                           available_plugins=list(plugin_registry.keys()))
            req = requests.post(self._url, data=msgpack.packb(
                payload, use_bin_type=True), **self._http_args)
            if req.ok:
                response = msgpack.unpackb(req.content, encoding='utf-8')

                if 'plugin' in response:
                    # Direct access
                    self._open_direct(response)
                    self.direct = True
                else:
                    # Proxied access
                    self._real_source = RemoteDataSourceProxied(
                        **self._init_args)
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
        if isinstance(self.dtype, bytes):
            self.dtype = pickle.loads(self.dtype)

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

    def _load_metadata(self):
        pass

    def _get_schema(self):
        pass


class RemoteDataSourceProxied(DataSource):
    def __init__(self, url, entry_name, container, user_parameters,
                 description, http_args):
        self._init_args = dict(
            url=url, entry_name=entry_name, container=container,
            user_parameters=user_parameters, description=description,
            http_args=http_args)

        self._url = url
        self._entry_name = entry_name
        self._user_parameters = user_parameters
        self._http_args = http_args

        self._source_id = None

        super(RemoteDataSourceProxied, self).__init__(container=container,
                                                      description=description)

    def _open_source(self):
        if self._source_id is None:
            payload = dict(action='open', name=self._entry_name,
                           parameters=self._user_parameters)
            req = requests.post(self._url, data=msgpack.packb(
                payload, use_bin_type=True), **self._http_args)
            if req.status_code == 200:
                response = msgpack.unpackb(req.content, encoding='utf-8')
                self._parse_open_response(response)

        return self._source_id

    def _parse_open_response(self, response):
        self.datashape = response['datashape']
        dtype_descr = response['dtype']
        if isinstance(dtype_descr, list):
            # Reformat because NumPy needs list of tuples
            dtype_descr = [tuple(x) for x in response['dtype']]
        if isinstance(dtype_descr, bytes):
            self.dtype = pickle.loads(dtype_descr)
        else:
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
            resp = requests.post(self._url, data=msgpack.packb(
                payload, use_bin_type=True), stream=True, **self._http_args)
            if resp.status_code != 200:
                raise Exception('Error reading data')

            for msg in msgpack.Unpacker(resp.raw, encoding='utf-8'):
                format = msg['format']
                compression = msg['compression']
                container = msg['container']

                compressor = serializer.compression_registry[compression]
                encoder = serializer.format_registry[format]
                chunk = encoder.decode(compressor.decompress(msg['data']),
                                       container=container)
                yield chunk
        finally:
            if resp is not None:
                resp.close()

    def _read(self, partition=None):
        chunks = list(self._get_chunks(partition=partition))
        return get_container_klass(self.container).read(chunks)

    def discover(self):
        self._open_source()
        return dict(datashape=self.datashape, dtype=self.dtype,
                    shape=self.shape, npartitions=self.npartitions)

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

    def _load_metadata(self):
        pass

    def _get_schema(self):
        pass
