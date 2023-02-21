import shlex
import subprocess
import time

import pytest

import intake

pytest.importorskip("tiled")

import httpx  # required by tiled, so will be here


@pytest.fixture()
def server():
    cmd = shlex.split("tiled serve pyobject --public tiled.examples.generated:tree")
    P = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    url = "http://localhost:8000"
    timeout = 20
    while True:
        try:
            r = httpx.get(url)
            if r.status_code == 200:
                break
        except:
            pass
        timeout -= 0.1
        if timeout < 0:
            P.terminate()
            out = P.communicate()
            raise RuntimeError("timeout waiting for Tiled server\n%s", out)
        time.sleep(0.1)
    yield url + "/api"
    P.terminate()
    P.wait()


def test_simple(server):
    cat = intake.open_tiled_cat(server)
    out = cat.tiny_image.read()
    assert out.shape
    assert out.all()
