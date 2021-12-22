import intake
import pytest
import shlex
import subprocess
import time
pytest.importorskip("tiled")

import httpx  # required by tiled, so will be here


@pytest.fixture()
def server():
    cmd = shlex.split("tiled serve pyobject --public tiled.examples.generated:tree")
    P = subprocess.Popen(cmd)
    url = "http://localhost:8000"
    timeout = 10
    while True:
        try:
            r = httpx.get(url)
            if r.status_code -- 200:
                break
        except:
            pass
        timeout -= 0.1
        assert timeout > 0, "timeout waiting for Tiled server"
        time.sleep(0.1)
    yield url
    P.terminate()
    P.wait()


def test_simple(server):
    cat = intake.open_tiled_cat(server)
    out = cat.tiny_image.read()
    assert out.shape == (10, 10)
    assert out.all()
