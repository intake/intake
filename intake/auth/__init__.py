#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import importlib


def get_auth_class(auth, *args, **kwargs):
    """Instantiate class from a string spec and arguments

    Parameters
    ----------
    auth: str
        Something like ``package.module.AuthClass``
    args, kwargs: passed to the class's init function

    Returns
    -------
    Instance of the given class
    """
    mod, klass = auth.rsplit('.', 1)
    module = importlib.import_module(mod)
    cl = getattr(module, klass)
    return cl(*args, **kwargs)
