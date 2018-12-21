import os
import sys

from conda_build.api import render

# This is needed because render spams the console
old_stdout = sys.stdout
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')

meta = render('conda')[0][0].meta

sys.stdout = old_stdout
print(' '.join(meta['test']['requires']))
