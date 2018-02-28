import functools
import itertools
import operator
import sys


def flatten(iterable):
    """Flatten an arbitrarily deep list"""
    iterable = iter(iterable)
    while True:
        try:
            item = next(iterable)
        except StopIteration:
            break

        if isinstance(item, (str, bytes)):
            yield item
            continue

        try:
            data = iter(item)
            iterable = itertools.chain(data, iterable)
        except:
            yield item


def reload_on_change(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.changed:
            self.reload()
        return f(self, *args, **kwargs)

    return wrapper


def clamp(value, lower=0, upper=sys.maxsize):
    """Clamp float between given range"""
    return max(lower, min(upper, value))


def make_prefix_tree(flat_dict):
    """Convert a dictionary with keys of dotted strings into nested dicts with common prefixes.

    Ex: { 'abc.xyz': 1, 'abc.def': 2, 'www': 3} is converted to:
        { 'abc': {'xyz': 1, 'def': 2}, 'www': 3 }
    """
    tree = {}
    for key, value in flat_dict.items():
        subtree = tree
        parts = key.split('.')
        for part in parts[:-1]:
            if part not in subtree:
                subtree[part] = {}
            subtree = subtree[part]
        subtree[parts[-1]] = value

    return tree


def flatten_dict(mapping, sep=None):
    def flatten(kv, path=None):
        if path is None:
            path = []
        for key in kv:
            newpath = path + [key]
            if isinstance(kv[key], dict):
                for u in flatten(kv[key], newpath):
                    yield u
            elif sep:
                yield sep.join(newpath), kv[key]
            else:
                yield tuple(newpath), kv[key]

    return dict(flatten(mapping))


def find_path(obj, path):
    return functools.reduce(operator.getitem, path, obj)
