import os
import numpy as np
import pytest
from ..npy import NPySource


@pytest.mark.parametrize('shape', [(1, ), (1, 1), (10, ), (5, 2), (3, 3, 3)])
def test_one_file(tmpdir, shape):
    size = 1
    for s in shape:
        size *= s
    data = np.random.randint(1, 100, size=size).reshape(shape)
    fn = os.path.join(tmpdir, 'out.npy')
    np.save(fn, data)
    s = NPySource(fn)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, chunks=1)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, shape=shape, dtype='int', chunks=1)
    out = s.read()
    assert (out == data).all()


@pytest.mark.parametrize('shape', [(1, ), (1, 1), (10, ), (5, 2), (3, 3, 3)])
def test_multi_file(tmpdir, shape):
    size = 1
    for s in shape:
        size *= s
    data0 = np.random.randint(1, 100, size=size).reshape(shape)
    fn0 = os.path.join(tmpdir, 'out0.npy')
    np.save(fn0, data0)
    data1 = np.random.randint(1, 100, size=size).reshape(shape)
    fn1 = os.path.join(tmpdir, 'out1.npy')
    np.save(fn1, data1)
    data = np.stack([data0, data1])
    fn = [fn0, fn1]
    s = NPySource(fn)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, chunks=1)
    out = s.read()
    assert (out == data).all()
    s = NPySource(fn, shape=shape, dtype='int', chunks=1)
    out = s.read()
    assert (out == data).all()
    s = NPySource(os.path.join(tmpdir, 'out*.npy'))
    out = s.read()
    assert (out == data).all()
