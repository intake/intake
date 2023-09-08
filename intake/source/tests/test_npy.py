# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import posixpath

import numpy as np
import pytest

import intake

from ..npy import NPySource

here = os.path.abspath(os.path.dirname(__file__))


@pytest.mark.parametrize("shape", [(1,), (1, 1), (10,), (5, 2), (3, 3, 3)])
def test_one_file(tempdir, shape):
    size = 1
    for s in shape:
        size *= s
    data = np.random.randint(1, 100, size=size).reshape(shape)
    fn = os.path.join(tempdir, "out.npy")
    np.save(fn, data)
    s = NPySource(fn)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, chunks=1)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, shape=shape, dtype="int", chunks=1)
    out = s.read()
    assert (out == data).all()


@pytest.mark.parametrize("shape", [(1,), (1, 1), (10,), (5, 2), (3, 3, 3)])
def test_multi_file(tempdir, shape):
    size = 1
    for s in shape:
        size *= s
    data0 = np.random.randint(1, 100, size=size).reshape(shape)
    fn0 = os.path.join(tempdir, "out0.npy")
    np.save(fn0, data0)
    data1 = np.random.randint(1, 100, size=size).reshape(shape)
    fn1 = os.path.join(tempdir, "out1.npy")
    np.save(fn1, data1)
    data = np.stack([data0, data1])
    fn = [fn0, fn1]

    s = NPySource(fn)
    out = s.read()
    assert (out == data).all()
    assert (s.to_dask().compute() == data).all()

    s = NPySource(fn, chunks=1)
    out = s.read()
    assert (out == data).all()
    assert (s.to_dask().compute() == data).all()

    s = NPySource(fn, shape=shape, dtype="int", chunks=1)
    out = s.read()
    assert (out == data).all()
    da = s.to_dask()
    assert (da.compute() == data).all()
    schema = s.discover()
    assert schema["shape"] == data.shape
    assert schema["npartitions"] == da.npartitions

    s = NPySource(os.path.join(tempdir, "out*.npy"))
    out = s.read()
    assert (out == data).all()
    assert (s.to_dask().compute() == data).all()


def test_zarr_minimal():
    pytest.importorskip("zarr")
    cat = intake.open_catalog(posixpath.join(here, "sources.yaml"))
    s = cat.zarr1()
    assert s.container == "ndarray"
    assert s.read().tolist() == [73, 98, 46, 38, 20, 12, 31, 8, 89, 72]
    assert s.npartitions == 1
    assert s.dtype.kind == "i"
    assert s.shape == (10,)
    assert (s.read_partition((0,)) == s.read()).all()


def test_zarr_parts():
    zarr = pytest.importorskip("zarr")
    out = {}
    g = zarr.open(out, mode="w")
    z = g.create_dataset("data", dtype="i4", shape=(10, 10), chunks=(5, 5), compression=None)
    z[:5, :5] = 1
    z[:5, 5:] = 2
    z[5:, :5] = 3
    z[5:, 5:] = 4
    source = intake.open_ndzarr(out, component="data")

    assert (source.read_partition((0, 0)) == 1).all()
    assert (source.read_partition((1, 1)) == 4).all()

    da = source.to_dask()
    assert da.npartitions == 4
    assert (da.compute() == z[:]).all()

    g = zarr.open(out, mode="w")
    gg = g.create_group("inner")
    z = gg.create_dataset("data", dtype="i4", shape=(10, 10), chunks=(5, 5), compression=None)
    z[:5, :5] = 1
    z[:5, 5:] = 2
    z[5:, :5] = 3
    z[5:, 5:] = 4
    source = intake.open_ndzarr(out, component="inner/data")

    assert (source.read_partition((0, 0)) == 1).all()
    assert (source.read_partition((1, 1)) == 4).all()

    da = source.to_dask()
    assert da.npartitions == 4
    assert (da.compute() == z[:]).all()
