"""Imports made my intake when it itself is imported

Since "plugins" are just subclasses of things like intake.readers.readers.BaseReader,
importing them is enough for registration.

To include imports from a package in the list of things to import, the canonical thing
to do is include an entry under "intake.imports" in the package entrypoints; the value
of each item will be imported.

The following config keys define behaviour:

- import_on_startup: if False, makes no automatic imports
- entrypoints_block_list: if an entrypoints import has a top-level package name
  in this list, it will be skipped
- import_extras: values in this list will be imported after processing entrypoints. This is
  a way to include imports without installing packages/entrypoints.
"""

from intake import conf, import_name, logger
from importlib.metadata import entry_points


def process_entries():
    eps = entry_points()
    if hasattr(eps, "select"):  # Python 3.10+ / importlib_metadata >= 3.9.0
        specs = eps.select(group="intake.imports")
    else:
        specs = eps.get("intake.imports", [])
    for spec in specs:
        top_level = spec.value.split(":", 1)[0].split(".", 1)[0]
        bl = conf.get("import_block_list", [])
        if top_level in bl or spec.name in bl:
            logger.debug("Skipping import of %s", spec)
            continue
        try:
            import_name(spec.value)
        except Exception as e:
            logger.warning(
                "Importing %s as part of processing intake entrypoints failed\n(%s)",
                spec.value,
                e,
            )
    for impname in conf["extra_imports"]:
        try:
            import_name(impname)
        except Exception as e:
            logger.warning(
                "Importing %s as part of processing intake extra_imports failed\n(%s)",
                impname,
                e,
            )


if conf["import_on_startup"]:
    process_entries()
