#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from __future__ import print_function

import os

install_local_data_files =  "{{cookiecutter.install_local_data_files}}" == "yes"

print("Don't forget to edit {{cookiecutter.package_name}}/{{cookiecutter.dataset_name}}.yaml to add your data sources!")

if install_local_data_files:
    os.mkdir('src')
    print("Put your data files in the {{cookiecutter.package_name}}/src/ directory to be included in the package.")
