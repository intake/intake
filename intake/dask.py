import dask.dataframe as dd
import dask.array as da
import dask.bag as db


def to_dask(dataref, **kwargs):
    if dataref.container == 'ndarray':
        return to_dask_array(dataref, **kwargs)
    elif dataref.container == 'dataframe':
        return to_dask_dataframe(dataref, **kwargs)
    elif dataref.container == 'list':
        return to_dask_bag(dataref, **kwargs)
    else:
        raise ValueError('Unknown container type: %s' % dataref.container)


def to_dask_dataframe(dataref, **kwargs):
    chunksize = kwargs.get('chunksize', 100000)
    if 'chunksize' in kwargs:
        del kwargs['chunksize']
    return dd.from_pandas(dataref.read(), chunksize=chunksize, **kwargs)

def to_dask_array(dataref, **kwargs):
    return da.from_array(dataref.read(), **kwargs)

def to_dask_bag(dataref, **kwargs):
    return db.from_sequence(dataref.read(), **kwargs)
