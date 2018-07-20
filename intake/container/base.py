import msgpack
import requests
from intake.source.base import DataSource, Schema
from . import serializer
from intake import __version__


class RemoteSource(DataSource):
    version = __version__

    def __init__(self, url, headers, name, parameters, metadata=None, **kwargs):
        super(RemoteSource, self).__init__(self)
        self.url = url
        self.name = name
        self.parameters = parameters
        self.headers = headers
        self._source_id = None
        self.metadata = metadata or {}
        self._get_source_id()

    def _get_source_id(self):
        if self._source_id is None:
            payload = dict(action='open', name=self.name,
                           parameters=self.parameters)
            req = requests.post(self.url, data=msgpack.packb(
                payload, use_bin_type=True), **self.headers)
            if req.status_code == 200:
                response = msgpack.unpackb(req.content, encoding='utf-8')
                self._parse_open_response(response)

    def _parse_open_response(self, response):
        self.datashape = response['datashape']
        dtype_descr = response['dtype']
        if isinstance(dtype_descr, list):
            # Reformat because NumPy needs list of tuples
            dtype_descr = [tuple(x) for x in response['dtype']]
        self.dtype = dtype_descr
        self.shape = tuple(response['shape'] or ())
        self.npartitions = response['npartitions']
        self.metadata = response['metadata']
        self._schema = Schema(datashape=None, dtype=self.dtype,
                              shape=self.shape,
                              npartitions=self.npartitions,
                              extra_metadata=self.metadata)
        self._source_id = response['source_id']

    def _get_partition(self, i):
        pass

    def to_dask(self):
        pass


def get_partition(url, headers, source_id, container, partition):
    accepted_formats = list(serializer.format_registry.keys())
    accepted_compression = list(serializer.compression_registry.keys())
    payload = dict(action='read',
                   source_id=source_id,
                   accepted_formats=accepted_formats,
                   accepted_compression=accepted_compression)

    if partition is not None:
        payload['partition'] = partition

    try:
        resp = requests.post(url, data=msgpack.packb(
            payload, use_bin_type=True), **headers)
        if resp.status_code != 200:
            raise Exception('Error reading data')

        msg = msgpack.unpackb(resp.content, encoding='utf-8')
        format = msg['format']
        compression = msg['compression']
        compressor = serializer.compression_registry[compression]
        encoder = serializer.format_registry[format]
        chunk = encoder.decode(compressor.decompress(msg['data']),
                               container)
        return chunk
    finally:
        if resp is not None:
            resp.close()
