from .. import utils


def test_expand_templates():
    pars = {'a': "{{par}} hi"}
    context = {'b': 1, 'par': 'ho'}
    assert utils.expand_templates(pars, context)['a'] == 'ho hi'
    assert utils.expand_templates(pars, context, True)[1] == {'b'}


def test_expand_nested_template():
    pars = {'a': ["{{par}} hi"]}
    context = {'b': 1, 'par': 'ho'}
    assert utils.expand_templates(pars, context)['a'] == ['ho hi']
    assert utils.expand_templates(pars, context, True)[1] == {'b'}

    pars = {'a': {'k': {("{{par}} hi", )}}}
    context = {'b': 1, 'par': 'ho'}
    assert utils.expand_templates(pars, context)['a'] == {'k': {("ho hi", )}}
    assert utils.expand_templates(pars, context, True)[1] == {'b'}
