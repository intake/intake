#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import sys

# This is needed because render spams the console
old_stdout = sys.stdout
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')

sys.stdout = old_stdout

test_requires = [l.strip() for l in open('test_requirements.txt')]
requires = [l.strip() for l in open('requirements.txt') if not l.startswith("#")]

print(' '.join(requires + test_requires))
