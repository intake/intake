import os

import pytest

import intake
from intake.source.derived import MissingTargetError, PipelineStepError

catfile = os.path.join(
    os.path.dirname(__file__), "..", "..", "catalog", "tests", "catalog_alias.yml"
)


@pytest.fixture
def pipe_cat():
    catfile = os.path.join(os.path.dirname(__file__), "..", "tests", "pipeline.yaml")
    return intake.open_catalog(catfile)


def test_columns():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    df2 = cat.derive_cols.read()
    assert df1[["state", "slug"]].equals(df2)


def _pick_columns(df, columns):
    return df[columns]


def test_df_transform():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    df2 = cat.derive_cols_func.read()
    assert df1[["state", "slug"]].equals(df2)


def test_barebones():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    cat = intake.open_catalog(catfile)
    assert cat.barebones.read() == len(df1)


def test_other_cat():
    cat = intake.open_catalog(catfile)
    df1 = cat.other_cat.read()
    assert df1.columns.tolist() == ["name", "score"]


def test_pipeline_no_loc(pipe_cat):
    with pytest.raises(ValueError):
        pipe_cat.error_iloc.read()

    with pytest.raises(ValueError):
        pipe_cat.error_loc.read()


def test_pipeline_failed(pipe_cat):
    with pytest.raises(PipelineStepError):
        _ = pipe_cat.failed.read()


def test_pipeline_cols(pipe_cat):
    name_col = pipe_cat.one_column.read()
    assert name_col.to_list() == ["Alice", "Bob", "Charlie", "Eve"]

    two_cols = pipe_cat.two_columns.read()
    assert two_cols.values.tolist() == [
        ["Alice", 1],
        ["Bob", 2],
        ["Charlie", 3],
        ["Eve", 3],
    ]


def test_pipeline_accessor(pipe_cat):
    lower = pipe_cat.accessor.read()

    assert lower.tolist() == ["alice", "bob", "charlie", "eve"]


def test_pipeline_assign(pipe_cat):
    df = pipe_cat.assign.read()

    assert "lower" in df
    assert df["lower"].tolist() == ["alice", "bob", "charlie", "eve"]


def test_pipeline_assign_value(pipe_cat):
    df = pipe_cat.assign_value.read()

    assert "new_col" in df
    assert df["new_col"].tolist() == ["value"] * 4


def test_pipeline_concat(pipe_cat):
    concated_rows = pipe_cat.concat_rows.read()
    assert concated_rows.shape == (8, 3)

    concated_cols = pipe_cat.concat_cols.read()
    assert concated_cols.shape == (4, 6)


def test_pipeline_merge(pipe_cat):
    merged = pipe_cat.merged.read()
    assert merged.columns.to_list() == [
        "name_x",
        "score_x",
        "rank",
        "name_y",
        "score_y",
    ]
    assert merged.shape == (6, 5)


def test_pipeline_merge_fail(pipe_cat):
    with pytest.raises(MissingTargetError):
        _ = pipe_cat.merge_fail.read()


def test_pipeline_join(pipe_cat):
    join1 = pipe_cat.join1.read()
    assert join1.shape == (4, 6)
    assert join1.columns.to_list() == [
        "name",
        "score",
        "rank",
        "name1",
        "score1",
        "rank1",
    ]

    join_list = pipe_cat.join_list.read()
    assert join_list.shape == (4, 6)
    assert join_list.columns.to_list() == [
        "name",
        "score",
        "rank",
        "name1",
        "score1",
        "rank1",
    ]


def test_pipeline_join_fail(pipe_cat):
    with pytest.raises(MissingTargetError):
        _ = pipe_cat.join_fail.read()


def test_pipeline_func(pipe_cat):
    from .util import zscore

    df = pipe_cat.df.read()
    z = zscore(df.score)

    pipe = pipe_cat.func.read()
    assert z.equals(pipe)


def test_pipeline_apply(pipe_cat):
    from .util import reverse

    df = pipe_cat.df.read()
    s = pipe_cat.apply.read()

    assert s.equals(df.name.apply(reverse))


def test_groupby_apply(pipe_cat):
    df = pipe_cat.df.read()
    agg = pipe_cat.groupby_apply.read()

    assert agg.score.equals(df.groupby("rank")["score"].sum())


def test_groupby_transform(pipe_cat):
    lengths = pipe_cat.groupby_transform.read()

    assert lengths.score.to_list() == [1, 1, 2, 2]


def test_pipeline_dask(pipe_cat):
    df = pipe_cat.groupby_apply.read()
    ddf = pipe_cat.groupby_apply.to_dask()

    assert df.equals(ddf.compute())
