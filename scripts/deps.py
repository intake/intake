#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import sys

from conda_build.api import render

# This is needed because render spams the console
old_stdout = sys.stdout
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')

meta = render('conda')[0][0].meta

sys.stdout = old_stdout

test_requires = [l.strip() for l in open('test_requirements.txt')]

print(' '.join(meta['test']['requires'] + test_requires))
