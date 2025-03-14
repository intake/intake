import os

import pandas as pd
import pytest

from intake.readers import catalogs, datatypes, readers

pytest.importorskip("pytest_postgresql")
pytest.importorskip("psycopg")


@pytest.fixture  # uses pytest-postgresql
def postgres_with_data(postgresql):
    """Check main postgresql fixture."""
    cur = postgresql.cursor()
    cur.execute(
        "create table t_random as select s, md5(random()::text) from generate_Series(1,50) s;"
    )
    postgresql.commit()
    cur.close()
    return int(cur._conn.pgconn.port)  # this is the one I found to be dynamic


@pytest.mark.skipif(os.name == "nt", reason="`postgresql` does not work on Windows")
def test_pg_pandas(postgres_with_data):
    pytest.importorskip("psycopg2")
    pytest.importorskip("sqlalchemy")

    data = datatypes.SQLQuery(
        conn=f"postgresql://postgres@127.0.0.1:{postgres_with_data}/tests",
        query="t_random",
    )
    reader = readers.PandasSQLAlchemy(data)
    out = reader.read()
    assert len(out) == 50
    out = reader.discover()
    assert len(out) == 10


@pytest.mark.skipif(os.name == "nt", reason="`postgresql` does not work on Windows")
def test_pg_duck_with_pandas_input(postgres_with_data):
    data = datatypes.SQLQuery(
        conn=f"postgresql://postgres@127.0.0.1:{postgres_with_data}/tests",
        query="t_random",
    )
    reader = readers.DuckSQL(data)
    out = reader.read()
    assert len(out) == 50
    out = reader.discover()
    assert len(out) == 10


@pytest.fixture
def sqlite_with_data(tmpdir):
    """Check main postgresql fixture."""
    pytest.importorskip("sqlalchemy", minversion="2")
    import sqlite3

    fn = f"{tmpdir}/test.db"
    cnx = sqlite3.connect(fn)
    df = pd.DataFrame({"a": ["hi", "ho"] * 1000})
    df.to_sql(name="oi", con=cnx)
    return fn


def test_sqlite_pandas(sqlite_with_data):
    pytest.importorskip("pandas", minversion="2", reason="Not working on earlier version of pandas")
    data = datatypes.SQLQuery(conn=f"sqlite:///{sqlite_with_data}", query="oi")
    reader = readers.PandasSQLAlchemy(data)
    out = reader.read()
    assert len(out) == 2000
    out = reader.discover()
    assert len(out) == 10


def test_sqlite_duck_with_pandas_input(sqlite_with_data):
    data = datatypes.SQLQuery(conn=f"sqlite:///{sqlite_with_data}", query="oi")
    reader = readers.DuckSQL(data)
    out = reader.read()
    assert len(out) == 2000
    out = reader.discover()
    assert len(out) == 10


def test_pandas_duck_pandas(sqlite_with_data):
    pytest.importorskip("pandas", minversion="2", reason="Not working on earlier version of pandas")
    data = datatypes.SQLQuery(conn=f"sqlite:///{sqlite_with_data}", query="oi")
    reader = readers.PandasSQLAlchemy(data)
    comment_dict = {"args": [1, "2"], "kwargs": {"true": False}}
    reader2 = reader.PandasToDuck("out", comment=str(comment_dict))
    reader3 = reader2.DuckToPandas()
    out = reader3.read()
    assert out[:2].to_dict() == {"a": {0: "hi", 1: "ho"}, "index": {0: 0, 1: 1}}

    reader = datatypes.SQLQuery({}, "SELECT comment FROM duckdb_tables();").to_reader(
        reader="DuckSQL"
    )
    assert reader.read().fetchall() == [(str(comment_dict),)]


def test_cat(sqlite_with_data):
    pytest.importorskip("pandas", minversion="2", reason="Not working on earlier version of pandas")
    data = datatypes.Service(url=f"sqlite:///{sqlite_with_data}")
    reader = catalogs.SQLAlchemyCatalog(data)
    cat = reader.read()
    assert list(cat.data) == ["oi"]
    out = cat["oi"].to_reader(outtype="pandas").read()
    assert len(out) == 2000
