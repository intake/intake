from .. import utils


def test_make_prefix_tree():
    x = {'abc.xyz': 1, 'abc.def': 2, 'abc.www.yyy': 3, 'www': 4}
    assert utils.make_prefix_tree(x) == \
        {'abc': {'xyz': 1, 'def': 2, 'www': {'yyy': 3}}, 'www': 4}


def test_expand_templates():
    pars = {'a': "{{par}} hi"}
    context = {'b': 1, 'par': 'ho'}
    assert utils.expand_templates(pars, context)['a'] == 'ho hi'
    assert utils.expand_templates(pars, context, True)[1] == {'b'}
