import shlex
import subprocess
import time

import pytest

import intake.readers.datatypes

tiled = pytest.importorskip("tiled")


@pytest.fixture()
def tiled_server():
    t0 = time.time()
    cmd = "tiled serve demo --port 8901"
    fail = False
    P = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
    )
    while True:
        line = P.stderr.readline()
        if "api_key=" in line:
            url = line.lstrip().rstrip()
            # fails if another tiled server is already running
            yield url
            break
        time.sleep(0.01)
        if time.time() - t0 > 5:
            fail = True
            break
    P.kill()
    P.wait()
    if fail:
        raise RuntimeError("tiled fixture did not start")


def test_catalog_workflow(tiled_server):
    from tiled.queries import FullText

    data = intake.readers.datatypes.TiledService(tiled_server)
    node = data.to_reader("tiled")
    cat = intake.entry.Catalog()
    cat["cat"] = (
        node.TiledSearch(query=FullText("dog"))
        .TiledSearch(query=FullText("red"))
        .TiledNodeToCatalog
    )
    cat.to_yaml_file("memory://cat.yaml")
    cat2 = intake.entry.Catalog.from_yaml_file("memory://cat.yaml")
    with intake.conf.set(allow_pickle=True):
        # tiled query instances are pickled
        scat = cat2["cat"].read()
    assert "short_table" in scat
