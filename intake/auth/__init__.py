
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
