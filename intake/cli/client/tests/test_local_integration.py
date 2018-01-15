import os.path
import subprocess

TEST_CATALOG_YAML = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_list():
    cmd = ['python', '-m', 'intake.cli.client', 'list', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out == "entry1\nentry1_part\nuse_example1\n"


def test_full_list():
    cmd = ['python', '-m', 'intake.cli.client', 'list', '--full', TEST_CATALOG_YAML]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    expected = """\
[entry1] container=dataframe
[entry1] description=entry1 full
[entry1] direct_access=forbid
[entry1] user_parameters=[]
[entry1_part] container=dataframe
[entry1_part] description=entry1 part
[entry1_part] direct_access=allow
[entry1_part] user_parameters=[{'default': '1', 'allowed': ['1', '2'], 'type': u'str', 'name': u'part', 'description': u'part of filename'}]
[use_example1] container=dataframe
[use_example1] description=example1 source plugin
[use_example1] direct_access=forbid
[use_example1] user_parameters=[]
"""

    assert out == expected


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

    assert out == expected


def test_exists_pass():
    cmd = ['python', '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out == "True\n"


def test_exists_fail():
    cmd = ['python', '-m', 'intake.cli.client', 'exists', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    assert out == "False\n"


def test_discover():
    cmd = ['python', '-m', 'intake.cli.client', 'discover', TEST_CATALOG_YAML, 'entry1']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = process.communicate()

    expected = """\
{'npartitions': 2, 'dtype': dtype([('name', 'O'), ('score', '<f8'), ('rank', '<i8')]), \
'shape': (None,), 'datashape': None, 'metadata': {'foo': 'bar', 'bar': [1, 2, 3]}}
"""

    assert out == expected


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

    assert out == expected


def test_get_fail():
    cmd = ['python', '-m', 'intake.cli.client', 'get', TEST_CATALOG_YAML, 'entry2']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()

    assert "KeyError: 'entry2'" in err
