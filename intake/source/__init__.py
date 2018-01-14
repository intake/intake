from .discovery import autodiscover


__all__ = ['registry']

# Populate list of autodetected plugins
registry = autodiscover()

# FIXME: this plugin needs to eventually be retired
from . import csv
registry['csv'] = csv.Plugin()
