import dask.dataframe as dd
import dask.array as da
import dask.bag as db


def to_dask(source, **kwargs):
    if source.container == 'ndarray':
        return to_dask_array(source, **kwargs)
    elif source.container == 'dataframe':
        return to_dask_dataframe(source, **kwargs)
    elif source.container == 'list':
        return to_dask_bag(source, **kwargs)
    else:
        raise ValueError('Unknown container type: %s' % source.container)


def to_dask_dataframe(source, **kwargs):
    chunksize = kwargs.get('chunksize', 100000)
    if 'chunksize' in kwargs:
        del kwargs['chunksize']
    return dd.from_pandas(source.read(), chunksize=chunksize, **kwargs)

def to_dask_array(source, **kwargs):
    return da.from_array(source.read(), **kwargs)

def to_dask_bag(source, **kwargs):
    return db.from_sequence(source.read(), **kwargs)
