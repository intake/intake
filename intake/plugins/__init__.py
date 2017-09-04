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
    from . import hdf5_array
    registry['hdf5_array'] = hdf5_array.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add hdf5_array plugin')

try:
    from . import hdf5_dataframe
    registry['hdf5_dataframe'] = hdf5_dataframe.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add hdf5_dataframe plugin')

try:
    from . import postgres
    registry['postgres'] = postgres.Plugin()
except Exception as e:
    traceback.print_exc()
    print('Could not add hdf5 plugin')
