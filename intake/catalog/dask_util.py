import dask


@dask.delayed
def read_partition(source, i):
    return source.read_partition(i)


def to_dask(source):

    futures = [read_partition(source, i) for i in range(source.npartitions)]

    if source.container == 'ndarray':
        # array_parts = [da.from_delayed(f, shape=c.shape, dtype=c.dtype) for f, c in zip(futures, chunks)]
        # return da.concatenate(array_parts, axis=0)
        raise ValueError('FIXME: Support ndarray concatenation')
    elif source.container == 'dataframe':
        import dask.dataframe as dd
        return dd.from_delayed(futures)
    elif source.container == 'list':
        import dask.bag as db
        return db.from_delayed(futures)
    else:
        raise ValueError('Unknown container type: %s' % source.container)
