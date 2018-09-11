import pytest


def verify_plugin_interface(plugin):
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)


@pytest.fixture
def temp_cache(tmpdir):
    import intake
    old = intake.config.conf.copy()
    olddir = intake.config.confdir
    intake.config.confdir = str(tmpdir)
    intake.config.conf.update({'cache_dir': str(tmpdir),
                               'cache_download_progress': False,
                               'cache_disabled': False})
    intake.config.save_conf()
    try:
        yield
    finally:
        intake.config.confdir = olddir
        intake.config.conf.update(old)
