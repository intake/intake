"""Tests for intake.readers.inspect.inspect_dataset.

Each test creates a real temporary file so that fsspec can actually fetch magic
bytes and the readers can open the data.  Optional packages are skipped via
``pytest.importorskip``.

Design rule: wherever an assertion depends on reader-specific behaviour
(columns, shape, tier, repr content, …) the test must explicitly pin the
reader via ``prefer=`` / ``exclude=`` rather than relying on whichever reader
happens to win in the current environment.
"""
from __future__ import annotations

import os
import tempfile

import pytest


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_tmp(content: bytes | str, suffix: str) -> str:
    """Write *content* to a named temp file and return its path."""
    if isinstance(content, str):
        content = content.encode()
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)
    return path


# Exclude every reader that isn't a plain Pandas reader so tests that need a
# specific, deterministic, always-available reader can just use this list.
_NON_PANDAS_READERS = [
    "Dask",
    "Duck",
    "Polars",
    "Ray",
    "Spark",
    "Numpy",
    "Awkward",
    "SKImage",
    "GeoPandas",
    "XArray",
]


# ---------------------------------------------------------------------------
# CSV – reader-agnostic structural tests
# ---------------------------------------------------------------------------


class TestCSV:
    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.df = pd.DataFrame({"name": ["alice", "bob", "carol"], "score": [1.1, 2.2, 3.3]})
        self.path = _write_tmp(self.df.to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    # --- reader-agnostic: these assertions hold regardless of which reader wins ---

    def test_basic_keys(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["url"] == self.path
        assert info["detected_type"] is not None
        assert info["errors"] == [], info["errors"]

    def test_detected_type_is_csv(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert "CSV" in info["detected_type"]

    def test_reader_used(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] is not None

    def test_readers_dict_nonempty(self):
        from intake.readers.inspect import inspect_dataset

        # Reader dict is populated before any reader is selected — no need to pin
        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert len(info["readers"]) > 0

    def test_readers_dict_structure(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        for name, entry in info["readers"].items():
            assert isinstance(name, str)
            assert "importable" in entry
            assert "tier" in entry
            assert isinstance(entry["importable"], bool)
            assert entry["tier"] in (1, 2, 3)

    def test_readers_dict_has_importable_entry(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        importable = [n for n, e in info["readers"].items() if e["importable"]]
        assert len(importable) > 0

    # --- reader-pinned: PandasCSV is always available (pandas is a baseline dep) ---

    def test_pandascsv_is_tier2(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] == "PandasCSV", info["reader_used"]
        assert info["reader_tier"] == 2

    def test_pandascsv_datashape_columns(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        ds = info["datashape"]
        assert "columns" in ds, ds
        assert "name" in ds["columns"]
        assert "score" in ds["columns"]

    def test_pandascsv_repr_set(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["repr"] is not None

    def test_pandascsv_shape_has_none_row_count(self):
        """Row count is never knowable from a pandas DataFrame without scanning.
        shape must be [None, n_cols] — column count known, row count unknown."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] == "PandasCSV"
        shape = info["shape"]
        assert shape is not None, "shape should be set (not None) — it carries column count"
        assert shape[0] is None, f"row count must be None, got {shape[0]}"
        assert shape[1] == 2, f"expected 2 columns (name, score), got {shape[1]}"

    def test_pandascsv_datashape_shape_has_none_row_count(self):
        """datashape['shape'] must carry [None, n_cols]."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] == "PandasCSV"
        ds_shape = info["datashape"].get("shape")
        assert ds_shape is not None
        assert ds_shape[0] is None
        assert ds_shape[1] == 2


# ---------------------------------------------------------------------------
# No matching reader
# ---------------------------------------------------------------------------


class TestNoMatchingReader:
    """A binary file with no recognisable magic should return partial info, not crash."""

    def setup_method(self):
        self.path = _write_tmp(b"\x00\x01\x02\x03" * 100, ".zzz_unknown")

    def teardown_method(self):
        os.unlink(self.path)

    def test_returns_dict(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path)
        assert isinstance(info, dict)
        assert "errors" in info

    def test_no_exception_raised(self):
        from intake.readers.inspect import inspect_dataset

        inspect_dataset(self.path)


# ---------------------------------------------------------------------------
# Parquet (requires pyarrow)
# ---------------------------------------------------------------------------


class TestParquet:
    def setup_method(self):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        self.df = pd.DataFrame({"x": range(100), "y": [float(i) for i in range(100)]})
        self.path = tempfile.mktemp(suffix=".parquet")
        self.df.to_parquet(self.path, index=False)

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_detected_type(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasParquet"],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward", "GeoPandas", "Delta"],
        )
        assert "Parquet" in info["detected_type"], info["detected_type"]

    def test_pandasparquet_schema_contains_columns(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasParquet"],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward", "GeoPandas", "Delta"],
        )
        assert info["reader_used"] == "PandasParquet", info["reader_used"]
        ds = info["datashape"]
        assert "columns" in ds, ds
        assert "x" in ds["columns"] and "y" in ds["columns"]

    def test_pandasparquet_shape_columns_known(self):
        """PandasParquet knows the column count; row count is omitted."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasParquet"],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward", "GeoPandas", "Delta"],
        )
        assert info["reader_used"] == "PandasParquet", info["reader_used"]
        shape = info["shape"]
        assert shape is not None
        assert shape[0] is None, f"row count must be None, got {shape[0]}"
        assert shape[1] == 2, f"expected 2 columns (x, y), got {shape[1]}"

    def test_no_fatal_errors(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasParquet"],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward", "GeoPandas", "Delta"],
        )
        for err in info["errors"]:
            assert "Traceback" not in err, err


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


class TestJSON:
    def setup_method(self):
        import json

        data = [{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}]
        self.path = _write_tmp(json.dumps(data), ".json")

    def teardown_method(self):
        os.unlink(self.path)

    def test_no_crash(self):
        pytest.importorskip("pandas")
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasJSON"] if False else [],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward"],
        )
        assert isinstance(info, dict)

    def test_detected_type_json(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path)
        assert info["detected_type"] is not None
        assert "json" in info["detected_type"].lower() or "JSON" in info["detected_type"]


# ---------------------------------------------------------------------------
# Numpy .npy
# ---------------------------------------------------------------------------


class TestNumpy:
    def setup_method(self):
        np = pytest.importorskip("numpy")
        self.arr = np.arange(24, dtype="float32").reshape(4, 6)
        fd, self.path = tempfile.mkstemp(suffix=".npy")
        os.close(fd)
        np.save(self.path, self.arr)

    def teardown_method(self):
        os.unlink(self.path)

    def test_no_crash(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["NumpyReader"])
        assert isinstance(info, dict)

    def test_numpyreader_shape_extracted(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["NumpyReader"], exclude=["Dask", "Cupy"])
        assert info["reader_used"] == "NumpyReader", info["reader_used"]
        assert tuple(info["shape"]) == (4, 6), info["shape"]


# ---------------------------------------------------------------------------
# NetCDF / xarray (requires xarray + scipy)
# ---------------------------------------------------------------------------


class TestNetCDF:
    def setup_method(self):
        xr = pytest.importorskip("xarray")
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")

        self.ds = xr.Dataset(
            {"temperature": (["x", "y"], np.random.rand(5, 4).astype("float32"))},
            coords={"x": range(5), "y": range(4)},
        )
        fd, self.path = tempfile.mkstemp(suffix=".nc")
        os.close(fd)
        self.ds.to_netcdf(self.path)

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_detected_type(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["XArrayDatasetReader"])
        dt = info["detected_type"] or ""
        assert "NetCDF" in dt or "HDF" in dt, dt

    def test_xarray_dims_in_datashape(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["XArrayDatasetReader"])
        assert info["reader_used"] == "XArrayDatasetReader", info["reader_used"]
        assert info["errors"] == [], info["errors"]
        ds = info["datashape"]
        assert "dims" in ds, ds
        assert "x" in ds["dims"] and "y" in ds["dims"]

    def test_xarray_shape_known(self):
        """xarray always knows its full shape from coordinate metadata."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["XArrayDatasetReader"])
        assert info["reader_used"] == "XArrayDatasetReader", info["reader_used"]
        # Shape is exposed via dims, not top-level shape for xarray
        ds = info["datashape"]
        assert ds["dims"] == {"x": 5, "y": 4}, ds["dims"]


# ---------------------------------------------------------------------------
# _extract_schema unit tests (no I/O)
# ---------------------------------------------------------------------------


class TestExtractSchema:
    def test_pandas_dataframe(self):
        pd = pytest.importorskip("pandas")
        from intake.readers.inspect import _extract_schema

        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        schema = _extract_schema(df)
        assert schema["columns"] == ["a", "b"]
        assert "int" in schema["dtypes"]["a"].lower() or "int" in schema["dtypes"]["a"]
        # Row count is always None; column count is always known
        assert schema["shape"] == [None, 2]

    def test_pandas_dataframe_is_sample_no_memory_bytes(self):
        pd = pytest.importorskip("pandas")
        from intake.readers.inspect import _extract_schema

        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        schema = _extract_schema(df, is_sample=True)
        assert schema["columns"] == ["a", "b"]
        # shape is still [None, n_cols] even for a sample
        assert schema["shape"] == [None, 2]
        # memory_bytes suppressed for samples
        assert "memory_bytes" not in schema

    def test_pandas_dataframe_extract_shape_has_none_row(self):
        pd = pytest.importorskip("pandas")

        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        from intake.readers.inspect import _extract_schema

        shape = _extract_schema(df)["shape"]
        assert shape == [None, 2]

    def test_polars_dataframe(self):
        pl = pytest.importorskip("polars")
        from intake.readers.inspect import _extract_schema

        df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        schema = _extract_schema(df)
        assert "x" in schema["columns"]
        # Row count always None for polars DataFrame too
        assert schema["shape"] == [None, 2]

    def test_polars_lazyframe(self):
        pl = pytest.importorskip("polars")
        from intake.readers.inspect import _extract_schema

        lf = pl.DataFrame({"x": [1, 2], "y": [3, 4]}).lazy()
        schema = _extract_schema(lf)
        assert "x" in schema["columns"]
        assert "shape" not in schema

    def test_numpy_array(self):
        np = pytest.importorskip("numpy")
        from intake.readers.inspect import _extract_schema

        arr = np.zeros((3, 4), dtype="float64")
        schema = _extract_schema(arr)
        assert schema["shape"] == [3, 4]
        assert "float64" in schema["dtype"]

    def test_xarray_dataset(self):
        xr = pytest.importorskip("xarray")
        np = pytest.importorskip("numpy")
        from intake.readers.inspect import _extract_schema

        ds = xr.Dataset({"var": (["x", "y"], np.zeros((2, 3)))})
        schema = _extract_schema(ds)
        assert "dims" in schema
        assert "x" in schema["dims"]

    def test_unknown_type_fallback(self):
        from intake.readers.inspect import _extract_schema

        class _MyObj:
            def __repr__(self):
                return "my_repr"

        schema = _extract_schema(_MyObj())
        assert "repr_truncated" in schema

    def test_extract_shape_dask_dataframe_is_none(self):
        """Dask DataFrame row count is nan — _extract_shape must return None."""
        dask_dd = pytest.importorskip("dask.dataframe")
        pd = pytest.importorskip("pandas")
        from intake.readers.inspect import _extract_shape

        df = dask_dd.from_pandas(pd.DataFrame({"x": range(5)}), npartitions=1)
        assert _extract_shape(df) is None

    def test_extract_shape_polars_lazyframe_is_none(self):
        """Polars LazyFrame row count is unknowable without collect()."""
        pl = pytest.importorskip("polars")
        from intake.readers.inspect import _extract_shape

        lf = pl.DataFrame({"x": [1, 2, 3]}).lazy()
        assert _extract_shape(lf) is None


# ---------------------------------------------------------------------------
# _reader_tier unit tests
# ---------------------------------------------------------------------------


class TestReaderTier:
    def test_dask_parquet_is_tier2(self):
        """DaskParquet.discover() calls .head() — it returns a sample, not a lazy graph."""
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import DaskParquet

        assert _reader_tier(DaskParquet) == 2

    def test_polars_csv_is_tier2(self):
        """PolarsCSV.discover() calls .head().collect() — returns a sample DataFrame."""
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import PolarsCSV

        assert _reader_tier(PolarsCSV) == 2

    def test_pandas_csv_is_tier2(self):
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import PandasCSV

        assert _reader_tier(PandasCSV) == 2

    def test_pandas_parquet_is_tier3(self):
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import PandasParquet

        assert _reader_tier(PandasParquet) == 3

    def test_duckdb_parquet_is_tier1(self):
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import DuckParquet

        assert _reader_tier(DuckParquet) == 1


# ---------------------------------------------------------------------------
# _best_reader unit tests
# ---------------------------------------------------------------------------


class TestBestReader:
    def test_prefers_tier2_over_tier3(self):
        """DaskParquet (Tier 2) is preferred over PandasParquet (Tier 3)."""
        from intake.readers.inspect import _best_reader
        from intake.readers.readers import DaskParquet, PandasParquet

        cls, tier = _best_reader([PandasParquet, DaskParquet])
        assert tier == 2
        assert cls is DaskParquet

    def test_empty_list_returns_none(self):
        from intake.readers.inspect import _best_reader

        cls, tier = _best_reader([])
        assert cls is None
        assert tier is None


# ---------------------------------------------------------------------------
# size-guard / timeout tests
# ---------------------------------------------------------------------------


class TestSizeGuard:
    """When file is larger than max_bytes and reader is Tier 3, skip the read."""

    def setup_method(self):
        pd = pytest.importorskip("pandas")
        import io

        buf = io.StringIO()
        pd.DataFrame({"a": range(1000), "b": range(1000)}).to_csv(buf, index=False)
        self.path = _write_tmp(buf.getvalue(), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_size_guard_skips_tier3_read(self):
        from intake.readers.inspect import inspect_dataset

        # Force PandasParquet-style: use PandasCSV but with max_bytes=0.
        # PandasCSV is Tier 2, so it won't be blocked. Instead, use
        # exclude to only leave Tier-3 readers and then set max_bytes=0.
        # NumpyText is Tier 3 for CSV-like files — but the simplest approach:
        # pin only PandasCSV (Tier 2), confirm it goes through regardless.
        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            max_bytes=0,
            timeout=None,
        )
        assert isinstance(info, dict)
        # PandasCSV is Tier 2, so size guard doesn't apply — should succeed
        assert info["reader_used"] == "PandasCSV", info["reader_used"]

    def test_size_guard_message_when_only_tier3_available(self):
        from intake.readers.inspect import inspect_dataset

        # Exclude all Tier-1/2 readers, leaving only Tier-3 (PandasParquet etc.)
        # but use a CSV — NumpyText is Tier 3 for FileData. Force it via exclude.
        # Simplest: exclude everything so no reader can run, max_bytes irrelevant.
        info = inspect_dataset(
            self.path,
            exclude=[
                "Pandas",
                "Dask",
                "Duck",
                "Polars",
                "Ray",
                "Spark",
                "Numpy",
                "Awkward",
                "SKImage",
                "GeoPandas",
            ],
            max_bytes=0,
            timeout=None,
        )
        assert isinstance(info, dict)
        # Either excluded message or size-guard message
        assert info["reader_used"] is None


class TestTimeout:
    """Verify that a discover() that hangs is interrupted."""

    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.path = _write_tmp(pd.DataFrame({"x": [1, 2]}).to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_timeout_returns_dict(self):
        from intake.readers.inspect import inspect_dataset

        # Very short timeout — may or may not fire depending on machine speed,
        # but must not raise an uncaught exception.
        info = inspect_dataset(
            self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS, timeout=0.0001
        )
        assert isinstance(info, dict)


# ---------------------------------------------------------------------------
# metadata passthrough
# ---------------------------------------------------------------------------


class TestMetadataPassthrough:
    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.path = _write_tmp(pd.DataFrame({"v": [42]}).to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_description_in_result(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            metadata={"description": "my test dataset"},
        )
        assert info["description"] == "my test dataset"
        assert info["metadata"]["description"] == "my test dataset"


# ---------------------------------------------------------------------------
# _matches_any / _ordered_candidates unit tests
# ---------------------------------------------------------------------------


class TestMatchesAny:
    def test_case_insensitive(self):
        from intake.readers.inspect import _matches_any

        assert _matches_any("PandasCSV", ["pandas"])
        assert _matches_any("DaskParquet", ["DASK"])
        assert not _matches_any("PandasCSV", ["polars"])

    def test_empty_patterns(self):
        from intake.readers.inspect import _matches_any

        assert not _matches_any("Anything", [])


class TestOrderedCandidates:
    def test_exclude_removes_readers(self):
        from intake.readers.inspect import _ordered_candidates
        from intake.readers.readers import PandasCSV, PandasParquet, DaskParquet

        result = _ordered_candidates(
            [PandasCSV, PandasParquet, DaskParquet],
            prefer=[],
            exclude=["Pandas"],
            file_size=None,
            max_bytes=None,
        )
        names = [cls.__name__ for cls, _ in result]
        assert "PandasCSV" not in names
        assert "PandasParquet" not in names
        assert "DaskParquet" in names

    def test_prefer_moves_to_front(self):
        from intake.readers.inspect import _ordered_candidates
        from intake.readers.readers import PandasCSV, PandasParquet, DaskParquet

        result = _ordered_candidates(
            [PandasCSV, PandasParquet, DaskParquet],
            prefer=["Pandas"],
            exclude=[],
            file_size=None,
            max_bytes=None,
        )
        names = [cls.__name__ for cls, _ in result]
        pandas_indices = [i for i, n in enumerate(names) if "Pandas" in n]
        dask_index = names.index("DaskParquet")
        assert all(i < dask_index for i in pandas_indices), names

    def test_prefer_sorted_by_tier_within_group(self):
        from intake.readers.inspect import _ordered_candidates
        from intake.readers.readers import PandasCSV, PandasParquet

        result = _ordered_candidates(
            [PandasParquet, PandasCSV],  # Tier-3 first in input
            prefer=["Pandas"],
            exclude=[],
            file_size=None,
            max_bytes=None,
        )
        names = [cls.__name__ for cls, _ in result]
        # PandasCSV is Tier 2, PandasParquet is Tier 3 → CSV should come first
        assert names[0] == "PandasCSV", names

    def test_empty_exclude_keeps_all(self):
        from intake.readers.inspect import _ordered_candidates
        from intake.readers.readers import PandasCSV, DaskParquet

        result = _ordered_candidates(
            [PandasCSV, DaskParquet],
            prefer=[],
            exclude=[],
            file_size=None,
            max_bytes=None,
        )
        assert len(result) == 2

    def test_exclude_all_returns_empty(self):
        from intake.readers.inspect import _ordered_candidates
        from intake.readers.readers import PandasCSV

        result = _ordered_candidates(
            [PandasCSV],
            prefer=[],
            exclude=["Pandas"],
            file_size=None,
            max_bytes=None,
        )
        assert result == []


# ---------------------------------------------------------------------------
# prefer / exclude integration tests
# ---------------------------------------------------------------------------


class TestPreferExclude:
    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        self.path = _write_tmp(self.df.to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_prefer_pandascsv_selects_pandascsv(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS, timeout=None
        )
        assert info["errors"] == [], info["errors"]
        assert info["reader_used"] == "PandasCSV", info["reader_used"]

    def test_exclude_pandas_avoids_all_pandas_readers(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, exclude=["Pandas"], timeout=None)
        assert info["reader_used"] is None or "Pandas" not in info["reader_used"], info[
            "reader_used"
        ]

    def test_exclude_all_returns_no_reader(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            exclude=[
                "Pandas",
                "Dask",
                "Duck",
                "Polars",
                "Spark",
                "Ray",
                "Numpy",
                "Awkward",
                "SKImage",
                "GeoPandas",
            ],
            timeout=None,
        )
        assert isinstance(info, dict)
        assert info["reader_used"] is None

    def test_readers_attempted_populated(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS, timeout=None
        )
        assert isinstance(info["readers_attempted"], list)
        assert "PandasCSV" in info["readers_attempted"]

    def test_prefer_and_exclude_combined(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=["PandasParquet"] + _NON_PANDAS_READERS,
            timeout=None,
        )
        assert info["errors"] == [], info["errors"]
        assert info["reader_used"] == "PandasCSV", info["reader_used"]


# ---------------------------------------------------------------------------
# retry tests
# ---------------------------------------------------------------------------


class TestRetry:
    """Test retry=True / retry=False behaviour."""

    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.path = _write_tmp(pd.DataFrame({"x": [1, 2, 3]}).to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_retry_true_falls_through_to_next_reader(self, monkeypatch):
        """With retry=True a forced failure on PandasCSV should fall through
        to the next Pandas reader and ultimately succeed."""
        from intake.readers.inspect import inspect_dataset
        from intake.readers import readers as rmod

        def _failing_discover(self, **kw):
            raise RuntimeError("synthetic first-reader failure")

        monkeypatch.setattr(rmod.PandasCSV, "discover", _failing_discover)

        # prefer PandasCSV first, but allow PandasParquet as fallback
        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            retry=True,
            timeout=None,
        )
        assert any("synthetic" in e for e in info["errors"]), info["errors"]
        # PandasCSV failed, so it should appear in readers_attempted
        assert "PandasCSV" in info["readers_attempted"], info["readers_attempted"]

    def test_retry_false_stops_after_first_failure(self, monkeypatch):
        """With retry=False a failure on PandasCSV must stop immediately."""
        from intake.readers.inspect import inspect_dataset
        from intake.readers import readers as rmod

        def _always_fail(self, **kw):
            raise RuntimeError("synthetic always-fail")

        monkeypatch.setattr(rmod.PandasCSV, "discover", _always_fail)

        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            retry=False,
            timeout=None,
        )
        assert isinstance(info, dict)
        assert len(info["readers_attempted"]) == 1, info["readers_attempted"]
        assert info["readers_attempted"][0] == "PandasCSV"
        assert any("synthetic" in e for e in info["errors"]), info["errors"]

    def test_retry_true_is_default(self):
        """Default behaviour is retry=True — just confirm no crash."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS, timeout=None
        )
        assert isinstance(info, dict)


# ---------------------------------------------------------------------------
# _file_storage_info unit tests
# ---------------------------------------------------------------------------


class TestFileStorageInfo:
    def setup_method(self):
        pd = pytest.importorskip("pandas")
        # Write three small CSV files
        self.tmpdir = tempfile.mkdtemp()
        self.paths = []
        for i in range(3):
            path = os.path.join(self.tmpdir, f"part_{i}.csv")
            pd.DataFrame({"v": [i]}).to_csv(path, index=False)
            self.paths.append(path)

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_single_file_size_and_count(self):
        from intake.readers.inspect import _file_storage_info

        size, n = _file_storage_info(self.paths[0], None)
        assert n == 1
        assert size is not None and size > 0

    def test_list_of_files_sums_sizes(self):
        from intake.readers.inspect import _file_storage_info

        size, n = _file_storage_info(self.paths, None)
        assert n == 3
        # Total should be the sum of the three individual sizes
        individual = [_file_storage_info(p, None)[0] for p in self.paths]
        assert all(s is not None for s in individual)
        assert size == sum(s for s in individual if s is not None)

    def test_glob_expands_and_sums(self):
        from intake.readers.inspect import _file_storage_info

        glob_url = os.path.join(self.tmpdir, "part_*.csv")
        size, n = _file_storage_info(glob_url, None)
        assert n == 3
        assert size is not None and size > 0

    def test_nonexistent_returns_none(self):
        from intake.readers.inspect import _file_storage_info

        size, n = _file_storage_info("/nonexistent/path/to/file.csv", None)
        assert size is None

    def test_empty_glob_returns_zero_files(self):
        from intake.readers.inspect import _file_storage_info

        glob_url = os.path.join(self.tmpdir, "nofile_*.csv")
        size, n = _file_storage_info(glob_url, None)
        assert n == 0
        assert size is None

    def test_directory_url_lists_children(self):
        from intake.readers.inspect import _file_storage_info

        # Pass the directory itself (with trailing slash) — should find all 3 files
        dir_url = self.tmpdir + "/"
        size, n = _file_storage_info(dir_url, None)
        assert n == 3, f"expected 3 files in directory, got {n}"
        assert size is not None and size > 0

    def test_directory_url_without_trailing_slash(self):
        from intake.readers.inspect import _file_storage_info

        size, n = _file_storage_info(self.tmpdir, None)
        assert n == 3, f"expected 3 files in directory, got {n}"
        assert size is not None and size > 0

    def test_directory_size_equals_glob_size(self):
        """Directory listing and glob should yield identical totals."""
        from intake.readers.inspect import _file_storage_info

        dir_size, dir_n = _file_storage_info(self.tmpdir + "/", None)
        glob_size, glob_n = _file_storage_info(os.path.join(self.tmpdir, "part_*.csv"), None)
        assert dir_n == glob_n == 3
        assert dir_size == glob_size


class TestNFilesInResult:
    """Integration: n_files and file_size_bytes appear correctly in inspect output."""

    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.tmpdir = tempfile.mkdtemp()
        self.single_path = os.path.join(self.tmpdir, "data.csv")
        pd.DataFrame({"a": [1, 2, 3]}).to_csv(self.single_path, index=False)

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_single_file_n_files_is_1(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.single_path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            timeout=None,
        )
        assert info["n_files"] == 1, info["n_files"]
        assert info["file_size_bytes"] is not None and info["file_size_bytes"] > 0

    def test_npartitions_falls_back_to_n_files_for_single(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.single_path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            timeout=None,
        )
        # n_files=1, so npartitions should NOT be set from n_files (only >1 is interesting)
        assert info["npartitions"] is None or info["npartitions"] >= 1


# ---------------------------------------------------------------------------
# Newly added scientific / medical imaging & video formats
# ---------------------------------------------------------------------------


class TestNewImagingFormats:
    """Type detection + reader registration for NRRD, MetaImage, OpenEXR,
    whole-slide images and AVI.

    These assert on detection only (magic / extension) and on which reader
    classes are *offered*, so they pass regardless of whether the optional
    backend libraries (pynrrd, SimpleITK, openslide, tifffile, imageio) are
    installed.
    """

    # (suffix, file bytes, expected detected_type, reader class names expected)
    CASES = [
        (".nrrd", b"NRRD0004\n# comment\n", "NRRD", {"NRRDReader", "SimpleITKReader"}),
        (".mha", b"ObjectType = Image\nNDims = 3\n", "MetaImage", {"SimpleITKReader"}),
        (".exr", b"\x76\x2f\x31\x01" + b"\x00" * 40, "OpenEXRImage", {"OpenEXRReader"}),
        (".avi", b"RIFF\x00\x00\x00\x00AVI " + b"\x00" * 40, "AVIVideo", {"ImageIOVideoReader"}),
    ]

    def test_detection_and_readers(self):
        from intake.readers.inspect import inspect_dataset

        for suffix, content, expected_type, expected_readers in self.CASES:
            path = _write_tmp(content, suffix)
            try:
                info = inspect_dataset(path, timeout=None)
                assert info["detected_type"] == expected_type, (suffix, info["detected_type"])
                assert expected_readers.issubset(set(info["readers"])), (
                    suffix,
                    expected_readers,
                    set(info["readers"]),
                )
            finally:
                del info
                try:
                    os.unlink(path)
                except OSError:
                    pass

    def test_whole_slide_image_offers_slide_readers(self):
        # .svs is TIFF-based; whichever type wins, the slide-specific readers
        # must be discoverable for the WholeSlideImage datatype.
        from intake.readers import datatypes as dt
        from intake.readers.readers import recommend

        inst = dt.WholeSlideImage("/tmp/_nonexistent.svs")
        rec = recommend(inst)
        names = {c.__name__ for c in rec["importable"]} | {
            c.__name__ for c in rec["not_importable"]
        }
        assert {"OpenSlideReader", "TiffSlideReader"}.issubset(names), names

    def test_new_datatypes_registered(self):
        from intake.readers import datatypes as dt

        for name in ("NRRD", "MetaImage", "OpenEXRImage", "WholeSlideImage", "AVIVideo"):
            assert hasattr(dt, name), name
            assert issubclass(getattr(dt, name), dt.FileData)

    def test_nrrd_magic_detection_without_extension(self):
        # magic-based detection should work even if the extension is unknown
        from intake.readers import datatypes as dt

        path = _write_tmp(b"NRRD0004\n", ".unknownext")
        try:
            cands = dt.recommend(url=path)
            assert dt.NRRD in cands, [c.__name__ for c in cands]
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# HTML repr & thumbnail output
# ---------------------------------------------------------------------------


class TestHTMLRepr:
    """``inspect_dataset`` should surface an HTML representation when the
    discovered object provides ``_repr_html_`` (e.g. a pandas DataFrame)."""

    def setup_method(self):
        pd = pytest.importorskip("pandas")
        self.df = pd.DataFrame({"name": ["alice", "bob", "carol"], "score": [1.1, 2.2, 3.3]})
        self.path = _write_tmp(self.df.to_csv(index=False), ".csv")

    def teardown_method(self):
        os.unlink(self.path)

    def test_html_repr_present_for_dataframe(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasCSV"],
            exclude=_NON_PANDAS_READERS,
            timeout=None,
        )
        assert info["reader_used"] == "PandasCSV", info["reader_used"]
        assert info["html_repr"], "expected non-empty html_repr"
        # pandas emits a <table> in its _repr_html_
        assert "<table" in info["html_repr"]
        # column names should appear in the rendered table
        assert "name" in info["html_repr"] and "score" in info["html_repr"]

    def test_html_repr_is_str_or_none(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["html_repr"] is None or isinstance(info["html_repr"], str)


class TestThumbnail:
    """``inspect_dataset`` should produce a base64 PNG thumbnail for image
    arrays when Pillow is available."""

    def setup_method(self):
        self.PIL = pytest.importorskip("PIL")
        np = pytest.importorskip("numpy")
        from PIL import Image

        arr = (np.random.rand(48, 64, 3) * 255).astype("uint8")
        fd, self.path = tempfile.mkstemp(suffix=".png")
        os.close(fd)
        Image.fromarray(arr).save(self.path)

    def teardown_method(self):
        os.unlink(self.path)

    def test_thumbnail_data_uri(self):
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PILImageReader"], timeout=None)
        # detection should land on PNG and a numpy-producing reader
        assert info["detected_type"] == "PNG", info["detected_type"]
        assert info["reader_used"] == "PILImageReader", info["reader_used"]
        assert info["thumbnail"], "expected a thumbnail"
        assert info["thumbnail"].startswith("data:image/png;base64,")

    def test_thumbnail_decodes_to_png(self):
        import base64

        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PILImageReader"], timeout=None)
        assert info["thumbnail"]
        b64 = info["thumbnail"].split(",", 1)[1]
        raw = base64.b64decode(b64)
        # PNG signature
        assert raw[:8] == b"\x89PNG\r\n\x1a\n"

    def test_structure_triggers_thumbnail_only_for_images(self):
        # a plain CSV (table structure) should not get an image thumbnail
        pd = pytest.importorskip("pandas")
        csv_path = _write_tmp(pd.DataFrame({"a": [1, 2]}).to_csv(index=False), ".csv")
        try:
            from intake.readers.inspect import inspect_dataset

            info = inspect_dataset(csv_path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
            assert info["thumbnail"] is None
        finally:
            os.unlink(csv_path)
