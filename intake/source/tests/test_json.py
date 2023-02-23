# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2021, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import json
import os

import pytest
from fsspec import open_files
from fsspec.utils import compressions

from intake.source.jsonfiles import JSONFileSource, JSONLinesFileSource

here = os.path.abspath(os.path.dirname(__file__))

EXTENSIONS = {compression: f".{extension}" for extension, compression in compressions.items()}


@pytest.fixture(params=[None, "gzip", "bz2"])
def json_file(request, tmp_path) -> str:
    data = {"hello": "world"}
    file_path = str(tmp_path / "1.json")
    file_path += EXTENSIONS.get(request.param, "")
    with open_files([file_path], mode="wt", compression=request.param)[0] as f:
        f.write(json.dumps(data))
    return file_path


@pytest.fixture(params=[None, "gzip", "bz2"])
def jsonl_file(request, tmp_path) -> str:
    data = [{"hello": "world"}, [1, 2, 3]]
    file_path = str(tmp_path / "1.jsonl")
    file_path += EXTENSIONS.get(request.param, "")
    with open_files([file_path], mode="wt", compression=request.param)[0] as f:
        f.write("\n".join(json.dumps(row) for row in data))
    return file_path


def test_jsonfile(json_file: str):
    j = JSONFileSource(json_file, text_mode=True, compression="infer")
    out = j.read()
    assert isinstance(out, dict)
    assert out["hello"] == "world"


def test_jsonfile_none(json_file: str):
    try:
        j = JSONFileSource(json_file, text_mode=True, compression=None)
        out = j.read()
        # compression=None should not raise exception for uncompressed files
        assert json_file.endswith(".json")
    except UnicodeDecodeError:
        # compression=None should raise exception for compressed files
        assert not json_file.endswith(".json")
        return
    assert isinstance(out, dict)
    assert out["hello"] == "world"


def test_jsonfile_discover(json_file: str):
    j = JSONFileSource(json_file, text_mode=True, compression=None)
    schema = j.discover()
    assert schema == {"dtype": None, "shape": None, "npartitions": 0, "metadata": {}}


def test_jsonlfile(jsonl_file: str):
    j = JSONLinesFileSource(jsonl_file, compression="infer")
    out = j.read()
    assert isinstance(out, list)

    assert isinstance(out[0], dict)
    assert out[0]["hello"] == "world"

    assert isinstance(out[1], list)
    assert out[1] == [1, 2, 3]


def test_jsonfilel_none(jsonl_file: str):
    try:
        j = JSONLinesFileSource(jsonl_file, compression=None)
        out = j.read()
        # compression=None should not raise exception for uncompressed files
        assert jsonl_file.endswith(".jsonl")
    except UnicodeDecodeError:
        # compression=None should raise exception for compressed files
        assert not jsonl_file.endswith(".jsonl")
        return
    assert isinstance(out, list)

    assert isinstance(out[0], dict)
    assert out[0]["hello"] == "world"

    assert isinstance(out[1], list)
    assert out[1] == [1, 2, 3]


def test_jsonfilel_discover(json_file: str):
    j = JSONLinesFileSource(jsonl_file, compression=None)
    schema = j.discover()
    assert schema == {"dtype": None, "shape": None, "npartitions": 0, "metadata": {}}


def test_jsonl_head(jsonl_file: str):
    j = JSONLinesFileSource(jsonl_file, compression="infer")
    out = j.head(1)
    assert isinstance(out, list)
    assert len(out) == 1
    assert out[0]["hello"] == "world"
