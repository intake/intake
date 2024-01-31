# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import pandas as pd
import pytest

import intake.catalog.utils as utils


def test_expand_templates():
    pars = {"a": "{{par}} hi"}
    context = {"b": 1, "par": "ho"}
    assert utils.expand_templates(pars, context)["a"] == "ho hi"
    assert utils.expand_templates(pars, context, True)[1] == {"b"}


def test_expand_nested_template():
    pars = {"a": ["{{par}} hi"]}
    context = {"b": 1, "par": "ho"}
    assert utils.expand_templates(pars, context)["a"] == ["ho hi"]
    assert utils.expand_templates(pars, context, True)[1] == {"b"}

    pars = {"a": {"k": {("{{par}} hi",)}}}
    context = {"b": 1, "par": "ho"}
    assert utils.expand_templates(pars, context)["a"] == {"k": {("ho hi",)}}
    assert utils.expand_templates(pars, context, True)[1] == {"b"}


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (None, pd.Timestamp("1970-01-01 00:00:00")),
        (1, pd.Timestamp("1970-01-01 00:00:00.000000001")),
        ("1988-02-24T13:37+0100", pd.Timestamp("1988-02-24 13:37+0100")),
        (
            {"__datetime__": True, "as_str": "1988-02-24T13:37+0100"},
            pd.Timestamp("1988-02-24T13:37+0100"),
        ),
    ],
)
def test_coerce_datetime(test_input, expected):
    assert utils.coerce_datetime(test_input) == expected


def test_flatten():
    assert list(utils.flatten([["hi"], ["oi"]])) == ["hi", "oi"]


@pytest.mark.parametrize(
    "value,dtype,expected",
    [
        (1, "int", 1),
        ("1", "int", 1),
        (1, "str", "1"),
        ((), "list", []),
        ((1,), "list", [1]),
        ((1,), "list[str]", ["1"]),
        ("{'a': 1}", "dict", {"a": 1}),
        ("{'a': '1'}", "dict", {"a": "1"}),
        ("[1, 2]", "list", [1, 2]),
        ("['1', '2']", "list", ["1", "2"]),
        ("[{'a': '1'}]", "list", [{"a": "1"}]),
        ("{'a': [1, 2]}", "dict", {"a": [1, 2]}),
        ("{'a': ['1', '2']}", "dict", {"a": ["1", "2"]}),
        ("1988-02-24T13:37+0100", "datetime", pd.Timestamp("1988-02-24 13:37+0100")),
    ],
)
def test_coerce(value, dtype, expected):
    out = utils.coerce(dtype, value)
    assert out == expected
    assert type(out) == type(expected)
