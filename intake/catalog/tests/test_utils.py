#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import intake.catalog.utils as utils


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
