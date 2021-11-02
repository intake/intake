#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2021, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import json
import os
from pathlib import Path
from typing import Dict, Optional

import pytest
from fsspec import open_files
from intake.source.jsonfiles import JSONFileSource, JSONLinesFileSource

here = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def json_file_path(tmp_path) -> Path:
    file_path = tmp_path / "1.json"
    file_path.write_text('{"hello":"world"}')
    return file_path


@pytest.fixture
def json_file_compressed_paths(tmp_path) -> Dict[Optional[str], str]:
    data = {"hello": "world"}
    file_path = tmp_path / "1.json"
    compressed_paths = {}
    for compression in [None, "gzip", "bz2"]:
        path = str(file_path)
        if compression is not None:
            path = f"{file_path}.{compression}"
        with open_files([path], mode="wt", compression=compression)[0] as f:
            f.write(json.dumps(data))
        compressed_paths[compression] = path

    return compressed_paths


@pytest.fixture
def jsonl_file_path(tmp_path) -> Path:
    data = [{"hello": "world"}, [1,2,3]]
    file_path = tmp_path / "1.jsonl"
    file_path.write_text("\n".join(json.dumps(row) for row in data))
    return file_path


@pytest.fixture
def jsonl_file_compressed_paths(tmp_path) -> Dict[Optional[str], str]:
    data = [{"hello": "world"}, [1,2,3]]
    file_path = tmp_path / "1.json"
    compressed_paths = {}
    for compression in [None, "gzip", "bz2"]:
        path = str(file_path)
        if compression is not None:
            path = f"{file_path}.{compression}"
        with open_files([path], mode="wt", compression=compression)[0] as f:
            f.write("\n".join(json.dumps(row) for row in data))
        compressed_paths[compression] = path

    return compressed_paths



def test_jsonfile(json_file_path):
    j = JSONFileSource(str(json_file_path))
    out = j.read()
    assert isinstance(out, dict)
    assert out["hello"] == "world"


def test_jsonlfile(jsonl_file_path):
    j = JSONLinesFileSource(str(jsonl_file_path))
    out = j.read()
    assert isinstance(out, list)

    assert isinstance(out[0], dict)
    assert out[0]["hello"] == "world"

    assert isinstance(out[1], list)
    assert out[1] == [1, 2, 3]


@pytest.mark.parametrize('compression', [None, 'gzip', 'bz2'])
def test_compressed_json(json_file_compressed_paths: Dict[Optional[str], str], compression: Optional[str]):
    path = json_file_compressed_paths[compression]
    j = JSONFileSource(path, text_mode=True, compression=compression)
    out = j.read()
    assert isinstance(out, dict)
    assert out["hello"] == "world"


@pytest.mark.parametrize('compression', [None, 'gzip', 'bz2'])
def test_compressed_jsonl(jsonl_file_compressed_paths: Dict[Optional[str], str], compression: Optional[str]):
    path = jsonl_file_compressed_paths[compression]
    j = JSONLinesFileSource(path, text_mode=True, compression=compression)
    out = j.read()
    assert isinstance(out, list)

    assert isinstance(out[0], dict)
    assert out[0]["hello"] == "world"

    assert isinstance(out[1], list)
    assert out[1] == [1, 2, 3]


def test_jsonl_head(jsonl_file_path: Path):
    j = JSONLinesFileSource(jsonl_file_path)
    out = j.head(1)
    assert isinstance(out, list)
    assert len(out) == 1
    assert out[0]["hello"] == "world"
