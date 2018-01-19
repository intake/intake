import os.path
import subprocess

TEST_CATALOG_YAML = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_list():
    cmd = ['python', '-m', 'intake.cli.client', 'list', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert len(out.strip().split('\n')) == 3
    assert "entry1" in out
    assert "entry1_part" in out
    assert "use_example1" in out


def test_full_list():
    cmd = ['python', '-m', 'intake.cli.client', 'list', '--full', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert len(out.strip().split('\n')) == 12
    assert "[entry1]" in out
    assert "[entry1_part]" in out
    assert "[use_example1]" in out


def test_describe():
    cmd = ['python', '-m', 'intake.cli.client', 'describe', TEST_CATALOG_YAML, 'entry1']
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
    cmd = ['python', '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out.decode('utf-8') == "True\n"


def test_exists_fail():
    cmd = ['python', '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out.decode('utf-8') == "False\n"


def test_discover():
    cmd = ['python', '-m', 'intake.cli.client', 'discover', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()
    out = out.decode('utf-8')

    assert "'datashape':" in out
    assert "'dtype':" in out
    assert "'metadata':" in out
    assert "'npartitions':" in out
    assert "'shape':" in out


def test_get_pass():
    cmd = ['python', '-m', 'intake.cli.client', 'get', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    expected = """\
       name  score  rank
0    Alice1  100.5     1
1      Bob1   50.3     2
2  Charlie1   25.0     3
3      Eve1   25.0     3
4    Alice2  100.5     1
5      Bob2   50.3     2
6  Charlie2   25.0     3
7      Eve2   25.0     3
"""

    assert out.decode('utf-8') == expected


def test_get_fail():
    cmd = ['python', '-m', 'intake.cli.client', 'get', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, err = process.communicate()

    assert "KeyError: 'entry2'" in err.decode('utf-8')
