"""Automatic dataset inspection for intake readers.

Given any URL or query string, :func:`inspect_dataset` returns a structured
summary dict covering size, schema/dimensions, a text repr, an optional HTML
repr and thumbnail, and the list of readers that could (or could not) handle
the data — all while minimising the number of bytes actually read.

The function is **package-agnostic**: it interrogates
:func:`intake.readers.readers.recommend` rather than importing any optional
dependency directly.  If nothing can be loaded it still returns the type
detection results plus the list of missing packages.

Laziness tiers
--------------
Readers are ranked by how cheaply they can produce summary information:

* **Tier 1** – produces a *lazy graph* object whose schema is introspectable
  without any I/O: Dask, Polars LazyFrame, DuckDB, Spark, Ray.
* **Tier 2** – overrides ``discover()`` to read only a small sample: PandasCSV
  (10 rows), PandasSQLAlchemy (1 chunk), AwkwardParquet (first row-group), etc.
* **Tier 3** – ``discover()`` performs a full read.  Attempted only when the
  remote file size is below *max_bytes* (default 50 MB).

Reader selection and filtering
-------------------------------
``prefer`` and ``exclude`` accept substrings (case-insensitive) matched against
reader class names:

* ``prefer=["Polars", "Duck"]`` — move matching readers to the front of the
  candidate list (within their tier, they are tried first).
* ``exclude=["Spark", "Ray"]`` — remove matching readers from the candidate
  list entirely before any attempt is made.

When ``retry=True`` (default), if the chosen reader's ``discover()`` raises or
times out, the next candidate in the ordered list is tried automatically.  Set
``retry=False`` to surface the first failure immediately instead.

Usage
-----
>>> from intake.readers.inspect import inspect_dataset
>>> info = inspect_dataset("s3://my-bucket/data.parquet")
>>> info["datashape"]   # column names + dtypes dict
>>> info["shape"]       # (nrows, ncols) or array shape if available
>>> # Force DuckDB, fall back automatically on failure:
>>> info = inspect_dataset("data.csv", prefer=["Duck"])
>>> # Only try pandas readers, no retries:
>>> info = inspect_dataset("data.csv", prefer=["Pandas"], exclude=["Dask","Polars"], retry=False)
"""

from __future__ import annotations

import threading
import traceback
from typing import Any

# ---------------------------------------------------------------------------
# Tier classification
# ---------------------------------------------------------------------------

# output_instance fragments that indicate a lazy/graph object (Tier 1)
_TIER1_OUTPUT_FRAGMENTS = (
    "dask.",
    "dask:",
    "polars:LazyFrame",
    "duckdb:",
    "pyspark.",
    "pyspark:",
    "ray.data:",
    "dask_awkward:",
)

# Reader class-name fragments that explicitly override discover() with a
# partial/sample read (Tier 2).  These are identified by class name because
# the override is on the class itself.
_TIER2_CLASS_FRAGMENTS = (
    "PandasCSV",
    "DaskCSV",
    "DaskDF",
    "PandasSQLAlchemy",
    "AwkwardParquet",
    "DaskAwkwardParquet",
    "DaskAwkwardJSON",
    "DaskText",
    "MarkdownReader",  # reads only first 8 KB on discover()
    "Polars",  # covers PolarsCSV, PolarsParquet, etc. – all return LazyFrame
    "DuckDB",  # covers DuckParquet, DuckCSV, DuckJSON, DuckSQL
    "SparkDataFrame",  # covers SparkCSV, SparkParquet, etc.
    "Ray",  # covers RayParquet, RayCSV, etc.
)


def _reader_tier(reader_cls) -> int:
    """Return the laziness tier (1, 2, or 3) of a reader class.

    Tier 1 — ``discover()`` returns a lazy graph whose shape is already
    encoded in the graph metadata (Dask, Polars LazyFrame, DuckDB, …).

    Tier 2 — ``discover()`` returns a *sample* of the data (e.g. the first
    few rows via ``.head()``).  Class-name matching is checked **before** the
    output-instance fragment check: ``DaskDF`` subclasses inherit
    ``output_instance = "dask.dataframe:DataFrame"`` (which would score
    Tier 1) but their ``discover()`` returns a pandas ``.head()`` sample.

    Tier 3 — ``discover()`` triggers a full read.
    """
    from intake.readers.readers import DaskDF

    # DaskDF and all subclasses that inherit its head()-based discover()
    # are Tier 2, regardless of their output_instance.
    if issubclass(reader_cls, DaskDF) and "discover" not in reader_cls.__dict__:
        return 2
    name = reader_cls.__name__
    if any(name.startswith(f) or name == f for f in _TIER2_CLASS_FRAGMENTS):
        return 2
    oi = reader_cls.output_instance or ""
    if any(oi.startswith(f) for f in _TIER1_OUTPUT_FRAGMENTS):
        return 1
    return 3


def _best_reader(importable_readers: list) -> tuple[Any, int] | tuple[None, None]:
    """Pick the best (lowest tier) reader from the importable list.

    Returns ``(reader_cls, tier)`` or ``(None, None)`` if the list is empty.
    """
    if not importable_readers:
        return None, None
    best_cls = None
    best_tier = 4
    for cls in importable_readers:
        tier = _reader_tier(cls)
        if tier < best_tier:
            best_tier = tier
            best_cls = cls
    return best_cls, best_tier


def _matches_any(name: str, patterns: list[str]) -> bool:
    """Return True if *name* contains any of *patterns* (case-insensitive)."""
    name_lower = name.lower()
    return any(p.lower() in name_lower for p in patterns)


def _ordered_candidates(
    importable: list,
    prefer: list[str],
    exclude: list[str],
    file_size: int | None,
    max_bytes: int | None,
) -> list[tuple[Any, int]]:
    """Return an ordered list of ``(reader_cls, tier)`` pairs to attempt.

    Ordering rules (applied in sequence):

    1. Remove readers whose names match any *exclude* pattern.
    2. Split remaining readers into three groups:

       a. **preferred** — name matches any *prefer* pattern AND
          ``prefer_for_inspect`` is ``True``
       b. **normal** — ``prefer_for_inspect`` is ``True`` but name does not
          match *prefer*
       c. **display-only** — ``prefer_for_inspect`` is ``False`` (e.g.
          ``PanelImageViewer``); these readers produce output designed for
          interactive display and carry no queryable schema, so they are
          always tried last

    3. Within each group sort by tier (ascending), preserving original order
       for equal tiers.
    4. Concatenate: preferred → normal → display-only.
    5. Drop Tier-3 readers when *file_size* is known and exceeds *max_bytes*
       (they are kept in the list but will be skipped at attempt time with an
       explanatory error).

    Returns a list of ``(reader_cls, tier)`` tuples.
    """
    # Step 1: exclude
    candidates = [cls for cls in importable if not _matches_any(cls.__name__, exclude)]

    # Steps 2–4: split into preferred / normal / display-only, each sorted by tier
    preferred = [
        (cls, _reader_tier(cls))
        for cls in candidates
        if getattr(cls, "prefer_for_inspect", True) and _matches_any(cls.__name__, prefer)
    ]
    normal = [
        (cls, _reader_tier(cls))
        for cls in candidates
        if getattr(cls, "prefer_for_inspect", True) and not _matches_any(cls.__name__, prefer)
    ]
    display_only = [
        (cls, _reader_tier(cls))
        for cls in candidates
        if not getattr(cls, "prefer_for_inspect", True)
    ]

    preferred.sort(key=lambda x: x[1])
    normal.sort(key=lambda x: x[1])
    display_only.sort(key=lambda x: x[1])

    return preferred + normal + display_only


# ---------------------------------------------------------------------------
# Schema extraction helpers (no hard imports – all via getattr / type checks)
# ---------------------------------------------------------------------------


def _module_name(obj) -> str:
    return type(obj).__module__


def _class_name(obj) -> str:
    return type(obj).__name__


def _extract_schema(obj, is_sample: bool = False) -> dict:
    """Extract schema/shape information from a discovered object.

    Uses only attribute introspection so that none of the optional packages
    need to be imported in *this* module.  Returns a dict that is always
    JSON-serialisable (values are converted to str where necessary).

    Parameters
    ----------
    obj:
        The object returned by ``reader.discover()``.
    is_sample:
        ``True`` when *obj* is known to be a **partial sample** of the full
        dataset (e.g. the 10-row head from ``PandasCSV.discover()``).  When
        ``True``, any information that reflects only the sample (memory usage)
        is suppressed.

        Note: the row count (``shape[0]``) of tabular objects such as
        ``pandas.DataFrame`` is **always** reported as ``None`` regardless of
        *is_sample*, because the number of rows is never intrinsically
        knowable from the in-memory object alone — it requires either scanning
        all data or reading per-file metadata (e.g. Parquet footer row-counts).
        The column count (``shape[1]``) is always available and is reported.
        ``shape`` is always a ``list``, never a ``tuple``.
    """
    info: dict[str, Any] = {}
    mod = _module_name(obj)
    cls = _class_name(obj)

    # ---- pandas DataFrame / Series -----------------------------------------
    if "pandas" in mod and cls in ("DataFrame", "Series"):
        try:
            n_cols = len(obj.columns) if hasattr(obj, "columns") else None
            info["columns"] = list(obj.columns) if hasattr(obj, "columns") else None
            info["dtypes"] = (
                {str(k): str(v) for k, v in obj.dtypes.items()}
                if hasattr(obj, "dtypes") and hasattr(obj.dtypes, "items")
                else str(getattr(obj, "dtype", ""))
            )
            # Row count is not intrinsically knowable without scanning all data
            # (even a Tier-3 full read only tells us the rows *we read*).
            # shape[0] = None signals "unknown row count"; shape[1] = n_cols.
            info["shape"] = [None, n_cols]
            if not is_sample:
                try:
                    info["memory_bytes"] = int(obj.memory_usage(deep=True).sum())
                except Exception:
                    pass
        except Exception:
            pass

    # ---- dask DataFrame / Series -------------------------------------------
    # Match both classic dask (dask.dataframe) and dask-expr (_collection),
    # identified by having .npartitions and .columns/.dtype but not .chunks.
    elif (
        "dask" in mod
        and not ("array" in mod or "array" in cls.lower())
        and hasattr(obj, "npartitions")
        and not hasattr(obj, "chunks")
    ):
        try:
            info["columns"] = list(obj.columns) if hasattr(obj, "columns") else None
            info["dtypes"] = (
                {str(k): str(v) for k, v in obj.dtypes.items()}
                if hasattr(obj, "dtypes") and hasattr(obj.dtypes, "items")
                else str(getattr(obj, "dtype", ""))
            )
            info["npartitions"] = getattr(obj, "npartitions", None)
            # Row count is unknown (nan in classic dask, a lazy Scalar in
            # dask-expr) — omit shape entirely rather than misreporting it.
        except Exception:
            pass

    # ---- dask Array ---------------------------------------------------------
    elif "dask" in mod and "array" in mod:
        try:
            # dask.array always knows its full shape from the graph
            info["shape"] = list(obj.shape)
            info["dtype"] = str(obj.dtype)
            info["npartitions"] = getattr(obj, "npartitions", None)
            info["chunks"] = [list(c) for c in obj.chunks]
        except Exception:
            pass

    # ---- polars DataFrame ---------------------------------------------------
    elif "polars" in mod and cls == "DataFrame":
        try:
            schema = obj.schema
            n_cols = len(schema)
            info["columns"] = [str(k) for k in schema.keys()]
            info["dtypes"] = {str(k): str(v) for k, v in schema.items()}
            # Row count not reliably knowable without scanning — n_cols is in columns.
            info["shape"] = [None, n_cols]
            if not is_sample:
                try:
                    info["memory_bytes"] = int(obj.estimated_size())
                except Exception:
                    pass
        except Exception:
            pass

    # ---- polars LazyFrame ---------------------------------------------------
    elif "polars" in mod and cls == "LazyFrame":
        try:
            # obj.schema triggers a PerformanceWarning in newer Polars;
            # collect_schema() is the non-lazy equivalent introduced in Polars 0.20.
            collect_schema = getattr(obj, "collect_schema", None)
            schema = collect_schema() if callable(collect_schema) else obj.schema
            info["columns"] = [str(k) for k in schema.keys()]
            info["dtypes"] = {str(k): str(v) for k, v in schema.items()}
            # Row count is unknowable without collect() — intentionally omitted
        except Exception:
            pass

    # ---- DuckDB relation ----------------------------------------------------
    elif "duckdb" in mod:
        try:
            cols = getattr(obj, "columns", None)
            dtypes = getattr(obj, "dtypes", None)
            if cols and dtypes:
                info["columns"] = list(cols)
                info["dtypes"] = {str(c): str(d) for c, d in zip(cols, dtypes)}
            # Row count requires executing COUNT(*) — not done here
        except Exception:
            pass

    # ---- xarray Dataset -----------------------------------------------------
    elif "xarray" in mod and cls == "Dataset":
        try:
            # xarray always knows the full shape from its coordinate metadata.
            # Use .sizes (dict of dim→length) rather than .dims to avoid a
            # FutureWarning in newer xarray where .dims will return a set.
            info["dims"] = dict(obj.sizes)
            info["data_vars"] = {
                name: {
                    "dims": list(var.dims),
                    "shape": list(var.shape),
                    "dtype": str(var.dtype),
                }
                for name, var in obj.data_vars.items()
            }
            info["coords"] = list(obj.coords)
            if obj.attrs:
                info["attrs"] = {str(k): str(v) for k, v in list(obj.attrs.items())[:20]}
        except Exception:
            pass

    # ---- xarray DataArray ---------------------------------------------------
    elif "xarray" in mod and cls == "DataArray":
        try:
            # xarray always knows the full shape from its coordinate metadata
            info["dims"] = list(obj.dims)
            info["shape"] = list(obj.shape)
            info["dtype"] = str(obj.dtype)
            if obj.attrs:
                info["attrs"] = {str(k): str(v) for k, v in list(obj.attrs.items())[:20]}
        except Exception:
            pass

    # ---- numpy ndarray ------------------------------------------------------
    elif "numpy" in mod and cls == "ndarray":
        try:
            # Only a full read lands here (NumpyReader, SKImageReader, etc.)
            info["shape"] = list(obj.shape)
            info["dtype"] = str(obj.dtype)
            info["nbytes"] = int(obj.nbytes)
        except Exception:
            pass

    # ---- awkward Array ------------------------------------------------------
    elif "awkward" in mod:
        try:
            info["type"] = str(obj.type)
            info["ndim"] = int(obj.ndim)
            # length is the row-count of this object; suppress when partial
            if not is_sample:
                info["length"] = len(obj)
        except Exception:
            pass

    # ---- geopandas GeoDataFrame ---------------------------------------------
    elif "geopandas" in mod:
        try:
            n_cols = len(obj.columns)
            info["columns"] = list(obj.columns)
            info["dtypes"] = {str(k): str(v) for k, v in obj.dtypes.items()}
            # Row count not reliably knowable without scanning.
            info["shape"] = [None, n_cols]
            info["crs"] = str(getattr(obj, "crs", None))
            geom_col = getattr(obj, "geometry", None)
            if geom_col is not None:
                try:
                    info["geom_type"] = str(geom_col.geom_type.unique().tolist())
                except Exception:
                    pass
        except Exception:
            pass

    # ---- pyspark DataFrame --------------------------------------------------
    elif "pyspark" in mod and cls == "DataFrame":
        try:
            schema = getattr(obj, "schema", None)
            if schema is not None:
                info["columns"] = [f.name for f in schema.fields]
                info["dtypes"] = {f.name: str(f.dataType) for f in schema.fields}
            # Row count requires an action (collect/count) — not done here
        except Exception:
            pass

    # ---- ray Dataset --------------------------------------------------------
    elif "ray" in mod:
        try:
            schema = getattr(obj, "schema", lambda: None)()
            if schema is not None:
                names = getattr(schema, "names", None)
                types = getattr(schema, "types", None)
                if names and types:
                    info["columns"] = list(names)
                    info["dtypes"] = {str(n): str(t) for n, t in zip(names, types)}
            # count() requires a full scan — not done here
        except Exception:
            pass

    # ---- intake Catalog -----------------------------------------------------
    elif "intake" in mod and "Catalog" in cls:
        try:
            entries = getattr(obj, "entries", {})
            info["entry_count"] = len(entries)
            info["entry_names"] = list(entries.keys())[:50]
        except Exception:
            pass

    # ---- configparser.ConfigParser ------------------------------------------
    # Must appear before the generic dict/mapping branch because ConfigParser
    # also has .keys() / .values().
    elif "configparser" in mod and "ConfigParser" in cls:
        try:
            sections = obj.sections()
            info["sections"] = sections
            info["n_sections"] = len(sections)
            info["keys_per_section"] = {s: list(obj.options(s)) for s in sections}
        except Exception:
            pass

    # ---- plain dict / mapping -----------------------------------------------
    elif cls in ("dict", "OrderedDict") or (
        hasattr(obj, "keys") and hasattr(obj, "values") and not hasattr(obj, "dtypes")
    ):
        try:
            keys = list(obj.keys())
            info["keys"] = [str(k) for k in keys[:100]]
            info["n_keys"] = len(obj)
            info["value_types"] = {str(k): type(v).__name__ for k, v in list(obj.items())[:50]}
            nested = {
                str(k): list(v.keys())[:20]
                for k, v in list(obj.items())[:20]
                if isinstance(v, dict)
            }
            if nested:
                info["nested_keys"] = nested
        except Exception:
            pass

    # ---- plain str (Markdown, RST, plain text, etc.) -----------------------
    elif cls == "str" and mod == "builtins":
        try:
            lines = obj.splitlines()
            info["n_chars"] = len(obj)
            info["n_lines"] = len(lines)
            info["n_words"] = sum(len(line.split()) for line in lines)
            if is_sample:
                # Counts reflect only the head — mark them clearly
                info["n_chars_note"] = "partial head only"
                info["n_lines_note"] = "partial head only"
                info["n_words_note"] = "partial head only"
            headings = [ln for ln in lines if ln.startswith("#")]
            if headings:
                info["n_headings"] = len(headings)
                info["headings"] = [h.lstrip("#").strip() for h in headings[:10]]
                if is_sample:
                    info["headings_note"] = "from partial head only"
        except Exception:
            pass

    # ---- fallback -----------------------------------------------------------
    else:
        try:
            r = repr(obj)
            info["repr_truncated"] = r[:500] if len(r) > 500 else r
        except Exception:
            pass

    return info


def _extract_shape(obj, is_sample: bool = False) -> list | None:
    """Best-effort full-dataset shape extraction.

    Returns a ``list`` of integer dimensions, or ``None`` whenever the shape
    cannot be guaranteed to reflect the entire dataset:

    * *is_sample* is ``True`` (object is a partial read).
    * The object is a Polars LazyFrame, DuckDB relation, Spark DataFrame, or
      Ray Dataset (row count requires a full scan).
    * The object has a ``.shape`` where any dimension is not a plain ``int``
      (covers both old-style dask ``nan`` and new dask-expr ``Scalar`` objects).
    """
    if is_sample:
        return None

    mod = _module_name(obj)
    cls_name = _class_name(obj)

    # Types whose row count is intrinsically unknown without a full scan,
    # identified by module/class-name fragments.  Dask DataFrames are handled
    # below via the shape-element check rather than here, because the module
    # name varies across dask versions (dask.dataframe vs dask_expr._collection).
    _unknown_shape_types = (
        ("polars", "LazyFrame"),
        ("duckdb", ""),
        ("pyspark", ""),
        ("ray", ""),
        ("dask_awkward", ""),
    )
    for mod_frag, cls_frag in _unknown_shape_types:
        if mod_frag in mod and (not cls_frag or cls_frag in cls_name):
            return None

    try:
        s = getattr(obj, "shape", None)
        if s is None:
            return None
        # Only return a shape when every dimension is a plain integer.
        # Dask DataFrames (all versions) expose at least one non-int dimension:
        # either float nan (old) or a lazy Scalar object (dask-expr).
        dims = tuple(s)
        if not all(isinstance(d, int) for d in dims):
            return None
        return list(dims)
    except Exception:
        return None


def _html_repr(obj) -> str | None:
    """Try to get an HTML representation of *obj* without additional imports."""
    for method in ("_repr_html_", "_repr_svg_"):
        fn = getattr(obj, method, None)
        if fn is not None:
            try:
                result = fn()
                if isinstance(result, tuple):
                    result = result[0]
                if result:
                    return str(result)
            except Exception:
                pass
    return None


def _thumbnail(obj) -> str | None:
    """Try to produce a base64-encoded PNG thumbnail.

    Attempts (in order):
    1. ``_repr_png_()`` on the object itself (matplotlib figures, PIL images…)
    2. If the object is a numpy ndarray with 2-3 dims, attempt PIL thumbnail.
    3. If the object is an xarray DataArray or Dataset with exactly 2 spatial
       dims, attempt a matplotlib plot → PNG bytes.
    Returns a ``data:image/png;base64,…`` URI string or ``None``.
    """
    import base64

    # --- try native _repr_png_ first ----------------------------------------
    fn = getattr(obj, "_repr_png_", None)
    if fn is not None:
        try:
            raw = fn()
            if isinstance(raw, tuple):
                raw = raw[0]
            if raw:
                return "data:image/png;base64," + base64.b64encode(raw).decode()
        except Exception:
            pass

    mod = _module_name(obj)
    cls = _class_name(obj)

    # --- numpy image array --------------------------------------------------
    if "numpy" in mod and cls == "ndarray":
        try:
            import importlib

            PIL = importlib.import_module("PIL.Image")
            import io

            ndim = obj.ndim
            if ndim == 2 or (ndim == 3 and obj.shape[2] in (1, 3, 4)):
                arr = obj
                if ndim == 3 and arr.shape[2] == 1:
                    arr = arr[:, :, 0]
                img = PIL.fromarray(arr.astype("uint8") if arr.dtype != "uint8" else arr)
                img.thumbnail((256, 256))
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
        except Exception:
            pass

    # --- xarray 2-D plot -----------------------------------------------------
    if "xarray" in mod and cls in ("DataArray", "Dataset"):
        try:
            import io
            import importlib

            plt = importlib.import_module("matplotlib.pyplot")
            fig, ax = plt.subplots(figsize=(4, 3))
            if cls == "DataArray":
                obj.plot(ax=ax)
            else:
                # pick first data variable
                first_var = next(iter(obj.data_vars), None)
                if first_var:
                    obj[first_var].plot(ax=ax)
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight")
            plt.close(fig)
            return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Remote file-size helper
# ---------------------------------------------------------------------------


def _file_storage_info(
    url: str | list, storage_options: dict | None
) -> tuple[int | None, int | None]:
    """Return ``(total_bytes, n_files)`` for *url*.

    *url* may be:
    - a single path or remote URL (possibly a glob pattern or a directory),
    - a list of paths/URLs.

    Directories and glob patterns are both expanded to their constituent files
    via ``fs.ls()`` / ``fs.glob()``.  If the size of any individual file
    cannot be determined the total is returned as ``None`` (rather than a
    misleading partial sum).  ``n_files`` is always returned when the set of
    files can be enumerated, even if sizes are unavailable.
    """
    try:
        import fsspec

        def _resolve_to_files(fs, path: str) -> list[dict]:
            """Return a list of fsspec file-info dicts for *path*.

            Handles three cases:
            - glob pattern (contains *, ?, [) — expand with fs.glob()
            - directory (type=="directory" or path ends with /) — list contents
            - single file — return as-is
            """
            # Glob pattern
            if any(c in path for c in ("*", "?", "[")):
                expanded = fs.glob(path)
                if not expanded:
                    return []
                return [fs.info(p) for p in expanded]

            # Check what this path actually is
            try:
                entry = fs.info(path)
            except FileNotFoundError:
                return []

            entry_type = entry.get("type") or entry.get("Type") or ""
            is_dir = (
                entry_type == "directory" or path.rstrip("/").endswith("/") or path.endswith("/")
            )

            if is_dir:
                # List only immediate children that are files
                children = fs.ls(path.rstrip("/"), detail=True)
                files = [
                    c for c in children if (c.get("type") or c.get("Type") or "") != "directory"
                ]
                return files

            return [entry]

        # Normalise to a list of (fs, path) strings
        if isinstance(url, list):
            all_infos: list[dict] = []
            for u in url:
                fs, path = fsspec.core.url_to_fs(u, **(storage_options or {}))
                all_infos.extend(_resolve_to_files(fs, path))
        else:
            fs, path = fsspec.core.url_to_fs(url, **(storage_options or {}))
            all_infos = _resolve_to_files(fs, path)

        n_files = len(all_infos)
        if n_files == 0:
            return None, 0

        total = 0
        for info in all_infos:
            size = info.get("size") or info.get("Size") or info.get("ContentLength")
            if size is None:
                # Can't determine size for this entry — report count but not total
                return None, n_files
            total += int(size)

        return total, n_files
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Timeout wrapper (best-effort; uses threads so it works on all platforms)
# ---------------------------------------------------------------------------


class _TimeoutError(Exception):
    pass


def _run_with_timeout(fn, timeout_seconds: float | None):
    """Run *fn()* in the current thread with an optional wall-clock timeout.

    Uses a daemon thread + join; if *fn* hasn't returned by *timeout_seconds*
    the calling thread resumes and raises :class:`_TimeoutError`.  The
    background thread may continue running (no way to kill a Python thread
    cooperatively), but we simply ignore its result.
    """
    if timeout_seconds is None:
        return fn()

    result_box: list = [None]
    exc_box: list = [None]

    def _target():
        try:
            result_box[0] = fn()
        except Exception as e:  # noqa: BLE001
            exc_box[0] = e

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout_seconds)
    if t.is_alive():
        raise _TimeoutError(f"discover() timed out after {timeout_seconds}s")
    if exc_box[0] is not None:
        raise exc_box[0]
    return result_box[0]


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------


def inspect_dataset(
    url: str,
    storage_options: dict | None = None,
    max_bytes: int = 50_000_000,
    timeout: float | None = 30.0,
    metadata: dict | None = None,
    prefer: list[str] | None = None,
    exclude: list[str] | None = None,
    retry: bool = True,
) -> dict:
    """Inspect a dataset at *url* and return a summary dictionary.

    Parameters
    ----------
    url:
        Location of the data.  Any fsspec-compatible URL is accepted
        (``s3://``, ``gs://``, ``https://``, local path, …).
    storage_options:
        Keyword arguments forwarded to fsspec (credentials, etc.).
    max_bytes:
        Maximum file size (bytes) for which a Tier-3 (full-read) reader will
        be attempted.  Set to ``None`` to disable the guard entirely.
    timeout:
        Wall-clock seconds to allow for each ``discover()`` call.  ``None``
        disables the timeout.  Note: the background thread may continue after
        a timeout is triggered.
    metadata:
        Extra metadata dict merged into the ``BaseData`` instance.
    prefer:
        List of substring patterns (case-insensitive) matched against reader
        class names.  Matching readers are moved to the *front* of the
        candidate list (while still sorted by tier within the preferred group).
        Example: ``prefer=["Polars", "Duck"]``.
    exclude:
        List of substring patterns (case-insensitive).  Any reader whose class
        name contains one of these patterns is removed from the candidate list
        entirely before any attempt is made.
        Example: ``exclude=["Spark", "Ray"]``.
    retry:
        If ``True`` (default), when the chosen reader's ``discover()`` raises
        or times out the next candidate in the ordered list is tried
        automatically, continuing until one succeeds or the list is exhausted.
        If ``False``, the first failure is recorded and the function returns
        immediately without trying further readers.

    Returns
    -------
    dict with keys:

    ``url``
        The input URL.
    ``detected_type``
        Class name of the first matching ``BaseData`` subclass, or ``None``.
    ``detected_type_qname``
        Fully-qualified name (``"module:Class"``), or ``None``.
    ``structure``
        Set of structural tags from the datatype (e.g. ``{"table"}``).
    ``reader_used``
        Class name of the reader that ultimately succeeded, or ``None``.
    ``reader_tier``
        Integer 1/2/3 for the reader that succeeded, or ``None``.
    ``readers_attempted``
        Ordered list of reader class names that were tried (including failures).
    ``description``
        Value of ``metadata["description"]`` from the data instance, if any.
    ``datashape``
        Dict of schema information (columns + dtypes, or xarray dims, etc.).
        Does **not** include ``shape`` — that lives exclusively at the
        top-level ``shape`` key.
    ``shape``
        List of integer dimensions (e.g. ``[1000, 4]``), or ``None`` when
        the shape cannot be determined without a full scan (lazy DataFrames,
        partial reads, etc.).
    ``npartitions``
        Number of partitions as reported by the discovered object (Dask,
        Ray, etc.).  For file-based data with no in-memory partition count
        this falls back to ``n_files``.
    ``n_files``
        Number of individual files that make up the dataset (after glob
        expansion), or ``None`` if the URL is not file-based / unknowable.
    ``file_size_bytes``
        Total size in bytes across **all** files, or ``None`` if any file's
        size could not be determined or the URL is not file-based.
    ``repr``
        Plain-text ``repr()`` of the discovered object (capped at 1000 chars).
    ``html_repr``
        HTML string from ``_repr_html_()`` / ``_repr_svg_()``, or ``None``.
    ``thumbnail``
        ``data:image/png;base64,…`` URI, or ``None``.
    ``metadata``
        The ``metadata`` dict attached to the ``BaseData`` / ``BaseReader``.
    ``readers``
        Dict mapping every candidate reader class name to a sub-dict with
        keys ``"importable"`` (bool) and ``"tier"`` (int 1/2/3).  Whether a
        reader is importable reflects the *current* environment only; another
        machine may have different packages installed.
    ``errors``
        List of error strings for non-fatal problems encountered.
    """
    from intake.readers import datatypes as dt
    from intake.readers.readers import recommend as readers_recommend

    prefer = prefer or []
    exclude = exclude or []
    errors: list[str] = []

    result: dict[str, Any] = {
        "url": url,
        "detected_type": None,
        "detected_type_qname": None,
        "structure": set(),
        "reader_used": None,
        "reader_tier": None,
        "readers_attempted": [],
        "description": None,
        "datashape": {},
        "shape": None,
        "npartitions": None,
        "n_files": None,
        "file_size_bytes": None,
        "repr": None,
        "html_repr": None,
        "thumbnail": None,
        "metadata": {},
        "readers": {},
        "errors": errors,
    }

    # ------------------------------------------------------------------
    # Step 1: type detection (fetches ≤1 MB of magic bytes via fsspec)
    # ------------------------------------------------------------------
    try:
        data_candidates = dt.recommend(
            url=url,
            head=True,
            storage_options=storage_options,
        )
    except Exception as exc:
        errors.append(f"Type detection failed: {exc}")
        data_candidates = []

    if not data_candidates:
        errors.append("No matching intake data type found for this URL.")
        return result

    # Use the best (most specific) candidate as the primary data type.
    # If no importable reader is found for it, fall through to subsequent
    # candidates in order (e.g. NPZFile detected second after Excel).
    data_cls = data_candidates[0]
    result["detected_type"] = data_cls.__name__
    result["detected_type_qname"] = data_cls.qname()
    result["structure"] = set(data_cls.structure)

    # ------------------------------------------------------------------
    # Steps 2–3: instantiate each candidate in turn until we find one with
    # at least one importable reader.
    # ------------------------------------------------------------------
    data_instance = None
    importable: list = []
    not_importable: list = []

    for data_cls in data_candidates:
        # Instantiate
        try:
            from intake.readers.datatypes import FileData, Service

            if issubclass(data_cls, FileData):
                _instance = data_cls(
                    url=url,
                    storage_options=storage_options,
                    metadata=metadata or {},
                )
            elif issubclass(data_cls, Service):
                _instance = data_cls(url=url, metadata=metadata or {})
            else:
                _instance = data_cls(metadata=metadata or {})
        except Exception as exc:
            errors.append(f"Could not instantiate data type {data_cls.__name__}: {exc}")
            continue

        # Reader recommendations
        try:
            rec = readers_recommend(_instance)
            _importable = rec.get("importable", [])
            _not_importable = rec.get("not_importable", [])
        except Exception as exc:
            errors.append(f"Reader recommendation failed for {data_cls.__name__}: {exc}")
            _importable = []
            _not_importable = []

        # Accumulate the full reader dict across all candidates
        result["readers"].update(
            {cls.__name__: {"importable": True, "tier": _reader_tier(cls)} for cls in _importable}
        )
        result["readers"].update(
            {
                cls.__name__: {"importable": False, "tier": _reader_tier(cls)}
                for cls in _not_importable
            }
        )

        if _importable:
            # Found a usable candidate — use it
            data_instance = _instance
            importable = _importable
            not_importable = _not_importable
            result["detected_type"] = data_cls.__name__
            result["detected_type_qname"] = data_cls.qname()
            result["structure"] = set(data_cls.structure)
            break

        # No importable reader for this candidate — note it and try next
        # (not an error — normal fallthrough when multiple types match)
        result.setdefault("_fallthrough", []).append(
            f"Skipped {data_cls.__name__}: no importable reader."
        )
        not_importable = _not_importable  # keep last set for error message

    if data_instance is None or not importable:
        errors.append(
            "No importable reader found for any detected type. Install one of: "
            + ", ".join(cls.__name__ for cls in not_importable[:5])
        )
        return result

    result["metadata"] = dict(data_instance.metadata)
    result["description"] = data_instance.metadata.get("description")

    # ------------------------------------------------------------------
    # Step 4: file-storage info (total size + file count, used by Tier-3 guard)
    # ------------------------------------------------------------------
    file_size: int | None = None
    n_files: int | None = None
    if hasattr(data_instance, "url"):
        try:
            file_size, n_files = _file_storage_info(data_instance.url, storage_options)
        except Exception:
            pass
    result["file_size_bytes"] = file_size
    result["n_files"] = n_files

    # ------------------------------------------------------------------
    # Step 5: build ordered candidate list (prefer / exclude applied)
    # ------------------------------------------------------------------
    ordered = _ordered_candidates(importable, prefer, exclude, file_size, max_bytes)

    if not ordered:
        errors.append(
            "All importable readers were removed by exclude= filters. "
            f"Excluded patterns: {exclude}"
        )
        return result

    # ------------------------------------------------------------------
    # Step 6: attempt discover() in order, with optional retry
    # ------------------------------------------------------------------
    discovered = None

    for reader_cls, tier in ordered:
        # Tier-3 size guard
        if tier == 3 and max_bytes is not None and file_size is not None and file_size > max_bytes:
            errors.append(
                f"Skipping {reader_cls.__name__} (Tier 3): "
                f"file size {file_size:,} bytes exceeds max_bytes={max_bytes:,}."
            )
            if not retry:
                break
            continue

        # is_ok() guard — the reader's own suitability check (URL shape, file
        # content sniff, etc.).  is_ok() was already called by readers_recommend()
        # when building the importable list, but calling it again here means any
        # expensive content-sniffing logic in is_ok() only runs for the reader
        # classes that are actually about to be attempted, and makes the guard
        # explicit so future readers can rely on it unconditionally.
        try:
            ok = reader_cls.is_ok(data_instance)
        except Exception as exc:
            ok = False
            errors.append(f"Skipping {reader_cls.__name__}: is_ok() raised {exc}")
        if not ok:
            errors.append(
                f"Skipping {reader_cls.__name__}: is_ok() returned False for this data instance."
            )
            if not retry:
                break
            continue

        result["readers_attempted"].append(reader_cls.__name__)

        # Instantiate reader
        try:
            reader = reader_cls(data=data_instance)
        except Exception as exc:
            try:
                reader = reader_cls(data_instance)
            except Exception as exc2:
                errors.append(f"Could not instantiate {reader_cls.__name__}: {exc} / {exc2}")
                if not retry:
                    break
                continue

        # Call discover()
        try:
            discovered = _run_with_timeout(lambda: reader.discover(), timeout)
            # Cache the discovered object on the reader so that
            # reader.transform can call converter is_ok(discovered) at
            # pipeline-construction time without any further I/O.
            reader._discover_cache = discovered
            # Success – record which reader was used and stop
            result["reader_used"] = reader_cls.__name__
            result["reader_tier"] = tier
            break
        except _TimeoutError as exc:
            errors.append(f"{reader_cls.__name__}: {exc}")
        except Exception as exc:
            errors.append(
                f"discover() failed for {reader_cls.__name__}: "
                + "".join(traceback.format_exception_only(type(exc), exc)).strip()
            )

        if not retry:
            break

    if discovered is None:
        # Nothing worked; result already has errors populated
        return result

    # is_sample=True when the reader only returned a partial dataset (Tier 2).
    # Tier-1 objects are lazy graphs that already encode the full shape in their
    # metadata; Tier-3 is always a complete read.
    is_sample = result["reader_tier"] == 2

    # ------------------------------------------------------------------
    # Step 7: extract schema / shape from discovered object
    # ------------------------------------------------------------------
    try:
        result["datashape"] = _extract_schema(discovered, is_sample=is_sample)
    except Exception as exc:
        errors.append(f"Schema extraction failed: {exc}")

    try:
        # datashape["shape"] is the single source of truth.  _extract_shape
        # is used as a fallback for types whose _extract_schema branch does
        # not set it (e.g. raw objects reaching the fallback repr branch).
        shape = result["datashape"].get("shape") if result["datashape"] else None
        if shape is None:
            shape = _extract_shape(discovered, is_sample=is_sample)
        result["shape"] = shape
        result["npartitions"] = (
            result["datashape"].get("npartitions") if result["datashape"] else None
        )
        result["npartitions"] = result["npartitions"] or (
            n_files if n_files and n_files > 1 else None
        )
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Step 8: text repr
    # ------------------------------------------------------------------
    try:
        r = repr(discovered)
        result["repr"] = r[:1000] if len(r) > 1000 else r
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Step 9: HTML repr (best-effort)
    # ------------------------------------------------------------------
    try:
        result["html_repr"] = _html_repr(discovered)
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Step 10: thumbnail (best-effort, only for image/array types)
    # ------------------------------------------------------------------
    if result["structure"] & {"image", "array"}:
        try:
            result["thumbnail"] = _thumbnail(discovered)
        except Exception:
            pass

    return result
