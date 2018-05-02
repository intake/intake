
import importlib


def get_auth_class(auth, *args, **kwargs):
    mod, klass = auth.rsplit('.', 1)
    module = importlib.import_module(mod)
    cl = getattr(module, klass)
    return cl(*args, **kwargs)
