# Populate list of autodetected plugins
import traceback

registry = {}

from . import csv
registry['csv'] = csv.Plugin()

try:
    from . import postgres
    registry['postgres'] = postgres.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add hdf5 plugin')
