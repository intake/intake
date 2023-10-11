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
        if top_level in conf["entrypoint_block_list"] or spec.name in conf["entrypoint_block_list"]:
            logger.debug("Skipping import of %s", spec)
            continue
        try:
            import_name(spec.value)
        except Exception as e:
            logger.warning("Importing %s as part of processing intake entrypoints failed\n(%s)", spec.value, e)
    for impname in conf["extra_imports"]:
        try:
            import_name(impname)
        except Exception as e:
            logger.warning("Importing %s as part of processing intake extra_imports failed\n(%s)", impname, e)


if conf["import_on_startup"]:
    process_entries()
