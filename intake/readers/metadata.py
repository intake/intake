"""Some types and meanings of fields that can be expected in metadata dictionaries

Metadata should be JSON-serializable.

We may decide to have different recommended keys for data versus readers/pipelines

For a possible schema we could decide to use, see
https://specs.frictionlessdata.io/data-resource/
"""
from __future__ import annotations

from typing import List

metadata_fields = {
    "description": (str, "one-line description of the dataset"),
    "text": (str, "long-form prose description of the dataset"),
    "timestamp": (
        str,
        "most recent datum in the set, ISO format",
    ),  # timespan would be in "data" as an extent
    "imports": (List[str], "top-level packages needed to read this"),
    "environment": (str, "YAML string or URL of a conda env spec"),  # or requirements.txt
    "references": (List[str], "URLs with further information relating to this"),
    "repr": (str, "string form of output"),
    "data": (
        dict,
        "any data-specific details, such as field types, missing values bounds or statistics",
    ),
    "history": (
        List[dict],
        "Time-ordered list of operations done to get this data. Keys are ISO timestamps.",
    ),
    "datashape": (str, "if applicable, may have datashape, dtype(s), jsonschema or similar"),
    "thumbnail": (str, "url location of an image, ideally PNG format"),
}
