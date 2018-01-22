import intake


def test_autoregister_open():
    assert hasattr(intake, 'open_csv')
