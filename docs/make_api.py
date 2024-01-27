import os
import sys
import intake


def run(path):
    fn = os.path.join(path, "source", "api2.rst")
    with open(fn, "w") as f:
        print(
            f"""
API Reference
=============

User Functions
--------------

.. autosummary::
    intake.config.Config
    intake.readers.datatypes.recommend
    intake.readers.convert.auto_pipeline
    intake.readers.entry.Catalog
    intake.readers.entry.DataDescription
    intake.readers.entry.ReaderDescription
    intake.readers.readers.recommend
    intake.readers.readers.reader_from_call

.. autoclass:: intake.config.Config
    :members:

.. autofunction:: intake.readers.datatypes.recommend

.. autofunction:: intake.readers.convert.auto_pipeline

.. autoclass:: intake.readers.entry.Catalog
    :members:

.. autoclass:: intake.readers.entry.DataDescription
    :members:

.. autoclass:: intake.readers.entry.ReaderDescription
    :members:

.. autofunction:: intake.readers.readers.recommend

.. autofunction:: intake.readers.readers.reader_from_call

Base Classes
------------

These may be subclassed by developers

.. autosummary::""",
            file=f,
        )
        bases = (
            "intake.readers.datatypes.BaseData",
            "intake.readers.readers.BaseReader",
            "intake.readers.convert.BaseConverter",
            "intake.readers.namespaces.Namespace",
            "intake.readers.search.SearchBase",
            "intake.readers.user_parameters.BaseUserParameter",
        )
        for base in bases:
            print("  ", base, file=f)
        print(file=f)
        for base in bases:
            print(
                f""".. autoclass:: {base}
   :members:
""",
                file=f,
            )

        print(
            """

Data Classes
------------

.. autosummary::""",
            file=f,
        )
        for cls in sorted(intake.readers.subclasses(intake.BaseData), key=lambda c: c.qname()):
            print("  ", cls.qname().replace(":", "."), file=f)
        print(
            """

Reader Classes
--------------

Includes readers, transformers, converters and output classes.

.. autosummary::""",
            file=f,
        )
        for cls in sorted(intake.readers.subclasses(intake.BaseReader), key=lambda c: c.qname()):
            print("  ", cls.qname().replace(":", "."), file=f)


if __name__ == "__main__":
    here = os.path.abspath(os.path.dirname(sys.argv[0]))
    run(here)
else:
    here = os.path.abspath(os.path.dirname(__file__))
