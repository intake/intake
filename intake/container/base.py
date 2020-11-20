#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import msgpack
import requests
from requests.compat import urljoin
from intake.source.base import DataSource, Schema
from . import serializer
from ..compat import unpack_kwargs, pack_kwargs
from intake import __version__

class RemoteSource(DataSource):
    """Base class for all DataSources living on an Intake server"""
    version = __version__

    def __init__(self, url, headers, name, parameters, metadata=None, **kwargs):
        """

        Parameters
        ----------
        url: str
            Address of the server
        headers: dict
            HTTP headers to sue in calls
        name: str
            handle to reference this data
        parameters: dict
            To pass to the server when it instantiates the data source
        metadata: dict
            Additional info
        kwargs: ignored
        """
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
            req = requests.post(urljoin(self.url, '/v1/source'),
                                data=msgpack.packb(payload, **pack_kwargs),
                                **self.headers)
            req.raise_for_status()
            response = msgpack.unpackb(req.content, **unpack_kwargs)
            self._parse_open_response(response)

    def _parse_open_response(self, response):
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
                              metadata=self.metadata)
        self._source_id = response['source_id']

    def _get_partition(self, i):
        raise NotImplementedError

    def to_dask(self):
        raise NotImplementedError


def get_partition(url, headers, source_id, container, partition):
    """Serializable function for fetching a data source partition

    Parameters
    ----------
    url: str
        Server address
    headers: dict
        HTTP header parameters
    source_id: str
        ID of the source in the server's cache (unique per user)
    container: str
        Type of data, like "dataframe" one of ``intake.container.container_map``
    partition: serializable
        Part of data to fetch, e.g., an integer for a dataframe.
    """
    accepted_formats = list(serializer.format_registry.keys())
    accepted_compression = list(serializer.compression_registry.keys())
    payload = dict(action='read',
                   source_id=source_id,
                   accepted_formats=accepted_formats,
                   accepted_compression=accepted_compression)

    if partition is not None:
        payload['partition'] = partition

    try:
        resp = requests.post(urljoin(url, '/v1/source'),
                             data=msgpack.packb(payload, **pack_kwargs),
                             **headers)
        if resp.status_code != 200:
            raise Exception('Error reading data')

        msg = msgpack.unpackb(resp.content, **unpack_kwargs)
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
