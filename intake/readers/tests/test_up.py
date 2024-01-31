import pytest


def test_basic():
    from intake.readers import user_parameters as up

    p = up.SimpleUserParameter(default=1, dtype=int)
    pars = {"k": ["{p}", 1]}
    out = up.set_values({"p": p}, pars)
    assert out == {"k": [1, 1]}

    pars = {"k": ["{p}", 1], "p": 2}
    out = up.set_values({"p": p}, pars)
    assert out == {"k": [2, 1]}

    # extra space here results in list member being string formatted
    pars = {"k": [" {p}", 1], "p": 2}
    out = up.set_values({"p": p}, pars)
    assert out == {"k": [" 2", 1]}

    with pytest.raises(TypeError):
        # supplied None as a value to int parameter
        pars = {"k": ["{p}", 1], "p": None}
        up.set_values({"p": p}, pars)
