# Populate list of autodetected plugins
from . import csv

registry = {}

registry['csv'] = csv.Plugin()
