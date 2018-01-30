from .. import utils


def test_make_prefix_tree():
    x = {'abc.xyz': 1, 'abc.def': 2, 'abc.www.yyy': 3, 'www': 4}
    assert utils.make_prefix_tree(x) == \
        {'abc': {'xyz': 1, 'def': 2, 'www': {'yyy': 3}}, 'www': 4}
