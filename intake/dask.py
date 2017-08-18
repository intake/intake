import dask.dataframe as dd
import dask.array as da
import dask.bag as db

from queue import Queue


def read_func(source):
    return source.read()


def scatter_chunks(source, chunksize, queue):
    with worker_client() as c:
        for chunk in source.read_chunks(chunksize=chunksize):
            future = c.scatter(chunk)
            queue.put(future)

def to_dask(source, client=None, **kwargs):
    chunksize = kwargs.get('chunksize', 100000)
    if 'chunksize' in kwargs:
        del kwargs['chunksize']

    if client is None:
        data = source.read()

        if source.container == 'ndarray':
            return da.from_array(source.read(), **kwargs)
        elif source.container == 'dataframe':
            return dd.from_pandas(data, chunksize=chunksize, **kwargs)
        elif source.container == 'list':
            return db.from_sequence(source.read(), **kwargs)
        else:
            raise ValueError('Unknown container type: %s' % source.container)
    else:
            # Strategy depends on source properties
        if source.get_chunks_supported:
            chunks = source.get_chunks(chunksize)
            futures = client.map(read_func, chunks)
        else:
            futures = client.map(read_func, [source])

        if source.container == 'ndarray':
            array_parts = [da.from_delayed(f, shape=c.shape, dtype=c.dtype) for f, c in zip(futures, chunks)]
            return da.concatenate(array_parts, axis=0)
        elif source.container == 'dataframe':
            return dd.from_delayed(futures)
        elif source.container == 'list':
            return db.from_delayed(futures)
        else:
            raise ValueError('Unknown container type: %s' % source.container)
