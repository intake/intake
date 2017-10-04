# Populate list of autodetected plugins
import traceback

registry = {}

from . import csv
registry['csv'] = csv.Plugin()
