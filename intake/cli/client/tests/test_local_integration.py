import os.path
import subprocess
import tempfile
import shutil
import sys

import pytest

from intake.util_tests import ex
TEST_CATALOG_YAML = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_list():
    cmd = [ex, '-m', 'intake.cli.client', 'list', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert len(out.strip().split('\n')) == 3
    assert "entry1" in out
    assert "entry1_part" in out
    assert "use_example1" in out


def test_full_list():
    cmd = [ex, '-m', 'intake.cli.client', 'list', '--full', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert len(out.strip().split('\n')) == 12
    assert "[entry1]" in out
    assert "[entry1_part]" in out
    assert "[use_example1]" in out


def test_describe():
    cmd = [ex, '-m', 'intake.cli.client', 'describe', TEST_CATALOG_YAML,
           'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    expected = """\
[entry1] container=dataframe
[entry1] description=entry1 full
[entry1] direct_access=forbid
[entry1] user_parameters=[]
"""

    assert out.decode('utf-8') == expected


def test_exists_pass():
    cmd = [ex, '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out.decode('utf-8') == "True\n"


def test_exists_fail():
    cmd = [ex, '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out.decode('utf-8') == "False\n"


def test_discover():
    cmd = [ex, '-m', 'intake.cli.client', 'discover', TEST_CATALOG_YAML,
           'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert "'datashape':" in out
    assert "'dtype':" in out
    assert "'metadata':" in out
    assert "'npartitions':" in out
    assert "'shape':" in out


def test_get_pass():
    cmd = [ex, '-m', 'intake.cli.client', 'get', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert 'Charlie1   25.0     3' in out.decode('utf-8')
    assert 'Eve2   25.0     3' in out.decode('utf-8')


def test_get_fail():
    cmd = [ex, '-m', 'intake.cli.client', 'get', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    _, err = process.communicate()

    assert "KeyError: 'entry2'" in err.decode('utf-8')

@pytest.fixture
def temp_current_working_directory():
    prev_cwd = os.getcwd()
    dirname = tempfile.mkdtemp()
    os.chdir(dirname)

    yield dirname

    os.chdir(prev_cwd)
    shutil.rmtree(dirname)


def test_example(temp_current_working_directory):
    cmd = [ex, '-m', 'intake.cli.client', 'example']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    assert process.returncode == 0
    assert os.path.exists('us_states.yml')
    assert os.path.exists('states_1.csv')
    assert os.path.exists('states_2.csv')
    
    # should fail second time due to existing files
    cmd = [ex, '-m', 'intake.cli.client', 'example']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    _, err = process.communicate()

    assert process.returncode > 0
