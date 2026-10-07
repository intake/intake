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


def test_named_options():
    from intake.readers import user_parameters as up

    p = up.NamedOptionsUserParameter({"a": "athing", "b": "bthing"}, default="b")
    pars = {"k": ["{p}", 1]}
    out = up.set_values({"p": p}, pars)
    assert out == {"k": ["bthing", 1]}

    pars = {"k": ["{p}", 1], "p": "a"}
    out = up.set_values({"p": p}, pars)
    assert out == {"k": ["athing", 1]}


@pytest.mark.parametrize(
    "min_value, max_value, good, bad",
    [
        (0, 1, [0, 0.5, 1], [-1, 2]),
        (-1, 0, [-1, -0.5, 0], [-2, 3]),
        (None, 0, [-10, 0], [0.5]),
        (0, None, [0, 10], [-0.5]),
    ],
)
def test_bounded_number(min_value, max_value, good, bad):
    from intake.readers import user_parameters as up

    p = up.BoundedNumberUserParameter(default=0, min_value=min_value, max_value=max_value)
    for value in good:
        assert p.validate(value)
        assert up.set_values({"p": p}, {"k": "{p}", "p": value}) == {"k": value}
    for value in bad:
        assert not p.validate(value)
        with pytest.raises(ValueError):
            up.set_values({"p": p}, {"k": "{p}", "p": value})
