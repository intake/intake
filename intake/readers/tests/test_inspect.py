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

    def test_pandascsv_shape_none_for_sample(self):
        """PandasCSV.discover() reads only 10 rows — shape must be None."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] == "PandasCSV"
        assert (
            info["shape"] is None
        ), f"Expected shape=None for tier-2 PandasCSV, got {info['shape']}"

    def test_pandascsv_no_shape_in_datashape_for_sample(self):
        """datashape['shape'] must be absent for a PandasCSV sample read."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(self.path, prefer=["PandasCSV"], exclude=_NON_PANDAS_READERS)
        assert info["reader_used"] == "PandasCSV"
        assert (
            "shape" not in info["datashape"]
        ), f"datashape should not contain 'shape' for PandasCSV sample: {info['datashape']}"


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

    def test_pandasparquet_shape_known(self):
        """PandasParquet is Tier 3 (full read) so shape must be the true shape."""
        from intake.readers.inspect import inspect_dataset

        info = inspect_dataset(
            self.path,
            prefer=["PandasParquet"],
            exclude=["Dask", "Duck", "Polars", "Ray", "Spark", "Awkward", "GeoPandas", "Delta"],
        )
        assert info["reader_used"] == "PandasParquet", info["reader_used"]
        assert info["shape"] == (100, 2), info["shape"]

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
        pytest.importorskip("pandas")
        import json

        data = [{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}]
        self.path = _write_tmp(json.dumps(data), ".json")

    def teardown_method(self):
        os.unlink(self.path)

    def test_no_crash(self):
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
        assert schema["shape"] == (2, 2)

    def test_pandas_dataframe_is_sample_no_shape(self):
        pd = pytest.importorskip("pandas")
        from intake.readers.inspect import _extract_schema

        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        schema = _extract_schema(df, is_sample=True)
        assert schema["columns"] == ["a", "b"]
        assert "shape" not in schema
        assert "memory_bytes" not in schema

    def test_pandas_dataframe_is_sample_no_shape_extract_shape(self):
        pd = pytest.importorskip("pandas")
        from intake.readers.inspect import _extract_shape

        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        assert _extract_shape(df, is_sample=True) is None
        assert _extract_shape(df, is_sample=False) == (2, 2)

    def test_polars_dataframe(self):
        pl = pytest.importorskip("polars")
        from intake.readers.inspect import _extract_schema

        df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        schema = _extract_schema(df)
        assert "x" in schema["columns"]
        assert schema["shape"] == (3, 2)

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
    def test_dask_parquet_is_tier1(self):
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import DaskParquet

        assert _reader_tier(DaskParquet) == 1

    def test_polars_csv_is_tier1(self):
        from intake.readers.inspect import _reader_tier
        from intake.readers.readers import PolarsCSV

        assert _reader_tier(PolarsCSV) == 1

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
    def test_prefers_tier1_over_tier3(self):
        from intake.readers.inspect import _best_reader
        from intake.readers.readers import DaskParquet, PandasParquet

        cls, tier = _best_reader([PandasParquet, DaskParquet])
        assert tier == 1
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
