import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import dask


@dask.delayed
def read_partition(source, i):
    return source.read_partition(i)


def to_dask(source):
    chunksize = 100000  # FIXME: Where should this come from?

    futures = [read_partition(source, i) for i in range(source.npartitions)]
    print(futures)

    if source.container == 'ndarray':
        #array_parts = [da.from_delayed(f, shape=c.shape, dtype=c.dtype) for f, c in zip(futures, chunks)]
        #return da.concatenate(array_parts, axis=0)
        raise ValueError('FIXME: Support ndarray concatenation')
    elif source.container == 'dataframe':
        return dd.from_delayed(futures)
    elif source.container == 'list':
        return db.from_delayed(futures)
    else:
        raise ValueError('Unknown container type: %s' % source.container)
