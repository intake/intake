# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os

import intake

here = os.path.abspath(os.path.dirname(__file__))
fn = os.path.join(here, "catalog_alias.yml")


def test_simple():
    cat = intake.open_catalog(fn)
    s = cat.alias0()
    assert s.container == "other"
    out = str(s.discover())
    assert s.container == "dataframe"
    assert "state" in out


def test_mapping():
    cat = intake.open_catalog(fn)
    s = cat.alias1()
    assert s.container == "other"
    out = str(s.discover())
    assert s.container == "dataframe"
    assert "state" in out

    s = cat.alias1(choice="second")
    assert s.container == "other"
    out = str(s.discover())
    assert s.container == "ndarray"
    assert "int64" in out


def test_other_cat():
    cat = intake.open_catalog(fn)
    other = cat.alias_other_cat

    assert other.source is None
    assert other.cat.name == cat.name

    _ = other.discover()

    assert other.source is not None
    assert other.source.cat.name == "name_in_cat"
    assert other.source._csv_kwargs == {"parse_dates": True}
    assert other.source.cat._captured_init_kwargs == {"getenv": True}


def test_alias():
    """make sure aliases are set up correctly"""

    cat = intake.entry.Catalog()

    filepath = os.path.join(here, "entry1_1.csv")
    data = intake.readers.datatypes.CSV(filepath)
    reader = intake.readers.readers.PandasCSV(data)

    # modify reader
    reader_modified = reader.name.lower()

    # create catalog entry
    cat["entry1_1"] = reader_modified

    # Make sure that only the key/alias/name is
    # in the list of entries and aliases
    assert list(cat) == ["entry1_1"]
    assert cat.aliases == {"entry1_1": "entry1_1"}
