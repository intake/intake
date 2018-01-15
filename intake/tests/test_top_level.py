import intake


def test_autoregister_open():
    assert hasattr(intake, 'open_csv')


def test_default_catalogs():
    # No assumptions about contents of these catalogs.
    # Just make sure they exist and don't raise exceptions
    list(intake.cat)
