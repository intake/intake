from intake.readers.datatypes import *  # noqa: F403
from intake.readers.readers import *  # noqa: F403
from intake.readers.convert import BaseConverter, Pipeline, auto_pipeline, path
from intake.readers.entry import DataDescription, ReaderDescription
from intake.readers.user_parameters import BaseUserParameter, SimpleUserParameter
import intake.readers.user_parameters
import intake.readers.transform
import intake.readers.output
import intake.readers.catalogs
import intake.readers.examples
from intake.readers.metadata import metadata_fields


def recommend(data, *args, reader=False, **kwargs):
    """Recommend datatypes or readers depending on what is passed

    Calls intake.readers.datatypes.recommend or intake.readers.readers.recommend
    Please see their respective docstrings; or, better, explicitly call the one
    which you intend to use.

    Parameters
    ----------
    reader: bool (False)
        If the "data" is a URL or other base string that can be used
        to recommend a datatype, setting this flag True will take the first
        guess and return the readers that can handle it.
    """
    if isinstance(data, intake.readers.datatypes.BaseData):
        return intake.readers.readers.recommend(data, *args, **kwargs)
    datat = intake.readers.datatypes.recommend(data, *args, **kwargs)
    if reader is False:
        return datat
    return datat[0](data).possible_outputs
