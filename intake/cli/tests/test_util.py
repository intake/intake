#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------


import pytest

# module under test
import intake.cli.util as m

# TODO
def test_print_entry_info():
    pass

def test_die(capsys):
    with pytest.raises(SystemExit):
        m.die("foo")
    out, err = capsys.readouterr()
    assert err == "foo\n"
    assert out == ""

class Test_nice_join(object):

    def test_default(self):
        assert m.nice_join(["one"]) == "one"
        assert m.nice_join(["one", "two"]) == "one or two"
        assert m.nice_join(["one", "two", "three"]) == "one, two or three"
        assert m.nice_join(["one", "two", "three", "four"]) == "one, two, three or four"

    def test_string_conjunction(self):
        assert m.nice_join(["one"], conjunction="and") == "one"
        assert m.nice_join(["one", "two"], conjunction="and") == "one and two"
        assert m.nice_join(["one", "two", "three"], conjunction="and") == "one, two and three"
        assert m.nice_join(["one", "two", "three", "four"], conjunction="and") == "one, two, three and four"

    def test_None_conjunction(self):
        assert m.nice_join(["one"], conjunction=None) == "one"
        assert m.nice_join(["one", "two"], conjunction=None) == "one, two"
        assert m.nice_join(["one", "two", "three"], conjunction=None) == "one, two, three"
        assert m.nice_join(["one", "two", "three", "four"], conjunction=None) == "one, two, three, four"

    def test_sep(self):
        assert m.nice_join(["one"], sep='; ') == "one"
        assert m.nice_join(["one", "two"], sep='; ') == "one or two"
        assert m.nice_join(["one", "two", "three"], sep='; ') == "one; two or three"
        assert m.nice_join(["one", "two", "three", "four"], sep="; ") == "one; two; three or four"

class TestSubcommand(object):

    def test_initialize_abstract(self):
        with pytest.raises(NotImplementedError):
            obj = m.Subcommand("parser")
            obj.initialize()

    def test_invoke_abstract(self):
        with pytest.raises(NotImplementedError):
            obj = m.Subcommand("parser")
            obj.invoke("args")
