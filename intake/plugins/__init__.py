# Populate list of autodetected plugins
import traceback

registry = {}

try:
    from . import csv
    registry['csv'] = csv.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add csv plugin')

try:
    from . import hdf5
    registry['hdf5'] = hdf5.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add hdf5 plugin')
