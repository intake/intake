from intake.readers.datatypes import *  # noqa: F403
from intake.readers.readers import *  # noqa: F403
from intake.readers.convert import BaseConverter, Pipeline, auto_pipeline, path
from intake.readers.entry import DataDescription, ReaderDescription
from intake.readers.user_parameters import BaseUserParameter, SimpleUserParameter
import intake.readers.user_parameters
import intake.readers.transform
import intake.readers.output
import intake.readers.catalogs
import intake.readers.importlist
import intake.readers.examples
from intake.readers.metadata import metadata_fields


def recommend(data, *args, **kwargs):
    """Recommend datatypes or readers depending on what is passed

    Calls intake.readers.datatypes.recommend or intake.readers.readers.recommend
    Please see their respective docstrings; or, better, explicitly call the one
    which you intend to use.
    """
    if isinstance(data, intake.readers.datatypes.BaseData):
        return intake.readers.readers.recommend(data, *args, **kwargs)
    return intake.readers.datatypes.recommend(data, *args, **kwargs)
