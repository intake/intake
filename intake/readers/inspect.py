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
    "Polars",  # covers PolarsCSV, PolarsParquet, etc. – all return LazyFrame
    "DuckDB",  # covers DuckParquet, DuckCSV, DuckJSON, DuckSQL
    "SparkDataFrame",  # covers SparkCSV, SparkParquet, etc.
    "Ray",  # covers RayParquet, RayCSV, etc.
)


def _reader_tier(reader_cls) -> int:
    """Return the laziness tier (1, 2, or 3) of a reader class."""
    oi = reader_cls.output_instance or ""
    if any(oi.startswith(f) for f in _TIER1_OUTPUT_FRAGMENTS):
        return 1
    name = reader_cls.__name__
    if any(name.startswith(f) or name == f for f in _TIER2_CLASS_FRAGMENTS):
        return 2
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
    2. Split remaining readers into *preferred* (name matches any *prefer*
       pattern) and *others*.
    3. Within each group sort by tier (ascending), preserving original order
       for equal tiers.
    4. Concatenate: preferred first, then others.
    5. Drop Tier-3 readers when *file_size* is known and exceeds *max_bytes*
       (they are kept in the list but will be skipped at attempt time with an
       explanatory error).

    Returns a list of ``(reader_cls, tier)`` tuples.
    """
    # Step 1: exclude
    candidates = [cls for cls in importable if not _matches_any(cls.__name__, exclude)]

    # Steps 2–4: split into preferred + others, each sorted by tier
    preferred = [
        (cls, _reader_tier(cls)) for cls in candidates if _matches_any(cls.__name__, prefer)
    ]
    others = [
        (cls, _reader_tier(cls)) for cls in candidates if not _matches_any(cls.__name__, prefer)
    ]

    preferred.sort(key=lambda x: x[1])
    others.sort(key=lambda x: x[1])

    return preferred + others


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
        dataset (e.g. the 10-row head from ``PandasCSV.discover()``, or the
        first row-group from ``AwkwardParquet.discover()``).  In that case
        row-counts / lengths that would normally be taken from the object are
        suppressed (set to ``None``) because they only reflect the sample, not
        the whole dataset.  Column names, dtypes, dims, and other structural
        metadata that do *not* depend on row count are always reported.
    """
    info: dict[str, Any] = {}
    mod = _module_name(obj)
    cls = _class_name(obj)

    # ---- pandas DataFrame / Series -----------------------------------------
    if "pandas" in mod and cls in ("DataFrame", "Series"):
        try:
            info["columns"] = list(obj.columns) if hasattr(obj, "columns") else None
            info["dtypes"] = (
                {str(k): str(v) for k, v in obj.dtypes.items()}
                if hasattr(obj, "dtypes") and hasattr(obj.dtypes, "items")
                else str(getattr(obj, "dtype", ""))
            )
            # Shape row-count is only meaningful when we have the full data.
            if not is_sample:
                info["shape"] = tuple(obj.shape)
                try:
                    info["memory_bytes"] = int(obj.memory_usage(deep=True).sum())
                except Exception:
                    pass
        except Exception:
            pass

    # ---- dask DataFrame / Series -------------------------------------------
    elif "dask" in mod and "dataframe" in mod:
        try:
            info["columns"] = list(obj.columns) if hasattr(obj, "columns") else None
            info["dtypes"] = (
                {str(k): str(v) for k, v in obj.dtypes.items()}
                if hasattr(obj, "dtypes") and hasattr(obj.dtypes, "items")
                else str(getattr(obj, "dtype", ""))
            )
            info["npartitions"] = getattr(obj, "npartitions", None)
            # Dask's shape[0] is nan (unknown row count) — preserve that
            # faithfully rather than misreporting it.
            shape = getattr(obj, "shape", None)
            if shape is not None:
                parts = tuple(str(s) for s in shape)
                # Only include if at least one dimension is actually known
                if any(s not in ("nan", "None", "unknown") for s in parts):
                    info["shape"] = parts
        except Exception:
            pass

    # ---- dask Array ---------------------------------------------------------
    elif "dask" in mod and "array" in mod:
        try:
            # dask.array always knows its full shape from the graph
            info["shape"] = tuple(obj.shape)
            info["dtype"] = str(obj.dtype)
            info["npartitions"] = getattr(obj, "npartitions", None)
            info["chunks"] = [list(c) for c in obj.chunks]
        except Exception:
            pass

    # ---- polars DataFrame ---------------------------------------------------
    elif "polars" in mod and cls == "DataFrame":
        try:
            schema = obj.schema
            info["columns"] = [str(k) for k in schema.keys()]
            info["dtypes"] = {str(k): str(v) for k, v in schema.items()}
            if not is_sample:
                info["shape"] = tuple(obj.shape)
                try:
                    info["memory_bytes"] = int(obj.estimated_size())
                except Exception:
                    pass
        except Exception:
            pass

    # ---- polars LazyFrame ---------------------------------------------------
    elif "polars" in mod and cls == "LazyFrame":
        try:
            schema = obj.schema
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
            # xarray always knows the full shape from its coordinate metadata
            info["dims"] = dict(obj.dims)
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
            info["columns"] = list(obj.columns)
            info["dtypes"] = {str(k): str(v) for k, v in obj.dtypes.items()}
            # GeoPandas readers do a full read, so shape is correct
            info["shape"] = tuple(obj.shape)
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

    # ---- fallback -----------------------------------------------------------
    else:
        try:
            r = repr(obj)
            info["repr_truncated"] = r[:500] if len(r) > 500 else r
        except Exception:
            pass

    return info


def _extract_shape(obj, is_sample: bool = False) -> tuple | list | None:
    """Best-effort full-dataset shape extraction.

    Returns ``None`` whenever the shape cannot be guaranteed to reflect the
    entire dataset:

    * *is_sample* is ``True`` (object is a partial read).
    * The object is a Dask DataFrame (row count is ``nan``).
    * The object is a Polars LazyFrame, DuckDB relation, Spark DataFrame, or
      Ray Dataset (row count requires a full scan).
    """
    mod = _module_name(obj)
    cls_name = _class_name(obj)

    # Types whose row count is intrinsically unknown without a full scan
    _unknown_shape_types = (
        ("dask", "dataframe"),  # dask DataFrame/Series
        ("polars", "LazyFrame"),
        ("duckdb", ""),
        ("pyspark", ""),
        ("ray", ""),
        ("dask_awkward", ""),
    )
    for mod_frag, cls_frag in _unknown_shape_types:
        if mod_frag in mod and (not cls_frag or cls_frag in cls_name):
            return None

    if is_sample:
        return None

    try:
        s = getattr(obj, "shape", None)
        if s is not None:
            return tuple(s)
    except Exception:
        pass
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


def _remote_file_size(url: str, storage_options: dict | None) -> int | None:
    """Return the byte size of a remote/local file, or None if unknowable."""
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(url, **(storage_options or {}))
        info = fs.info(path)
        size = info.get("size") or info.get("Size") or info.get("ContentLength")
        return int(size) if size is not None else None
    except Exception:
        return None


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
    ``shape``
        Tuple or list representing data shape, if available.
    ``npartitions``
        Number of Dask/Ray partitions, if available.
    ``file_size_bytes``
        Reported file size in bytes from the storage backend, or ``None``.
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

    # Use the best (most specific) candidate as the primary data type
    data_cls = data_candidates[0]
    result["detected_type"] = data_cls.__name__
    result["detected_type_qname"] = data_cls.qname()
    result["structure"] = set(data_cls.structure)

    # ------------------------------------------------------------------
    # Step 2: instantiate BaseData
    # ------------------------------------------------------------------
    try:
        from intake.readers.datatypes import FileData, Service

        if issubclass(data_cls, FileData):
            data_instance = data_cls(
                url=url,
                storage_options=storage_options,
                metadata=metadata or {},
            )
        elif issubclass(data_cls, Service):
            data_instance = data_cls(url=url, metadata=metadata or {})
        else:
            data_instance = data_cls(metadata=metadata or {})
    except Exception as exc:
        errors.append(f"Could not instantiate data type {data_cls.__name__}: {exc}")
        return result

    result["metadata"] = dict(data_instance.metadata)
    result["description"] = data_instance.metadata.get("description")

    # ------------------------------------------------------------------
    # Step 3: reader recommendations (no I/O)
    # ------------------------------------------------------------------
    try:
        rec = readers_recommend(data_instance)
        importable = rec.get("importable", [])
        not_importable = rec.get("not_importable", [])
    except Exception as exc:
        errors.append(f"Reader recommendation failed: {exc}")
        importable = []
        not_importable = []

    result["readers"] = {
        cls.__name__: {"importable": True, "tier": _reader_tier(cls)} for cls in importable
    }
    result["readers"].update(
        {cls.__name__: {"importable": False, "tier": _reader_tier(cls)} for cls in not_importable}
    )

    if not importable:
        errors.append(
            "No importable reader found. Install one of: "
            + ", ".join(cls.__name__ for cls in not_importable[:5])
        )
        return result

    # ------------------------------------------------------------------
    # Step 4: file-size check (used by Tier-3 guard below)
    # ------------------------------------------------------------------
    file_size: int | None = None
    if hasattr(data_instance, "url") and isinstance(data_instance.url, str):
        try:
            file_size = _remote_file_size(data_instance.url, storage_options)
        except Exception:
            pass
    result["file_size_bytes"] = file_size

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
        shape = _extract_shape(discovered, is_sample=is_sample)
        if shape is not None:
            result["shape"] = shape
        result["npartitions"] = result["datashape"].get("npartitions")
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
