
import os
import posixpath
import pytest
from intake.container.dataframe import GenericDataFrame
here = os.path.abspath(os.path.dirname(__file__))


def test_generic_dataframe():
    pd = pytest.importorskip('pandas')

    def make_a_part(openfile):
        return pd.DataFrame([[0]], columns=['x'])

    url = posixpath.join(here, '*.py')
    s = GenericDataFrame(url, reader=make_a_part)
    ddf = s.to_dask()
    assert ddf.compute().x.unique() == [0]
    df = s.read()
    assert len(df) == len(ddf)
