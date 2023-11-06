# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
from packaging.version import Version

gl = globals()


def do_import():
    try:
        import hvplot
        import panel as pn

        error = Version(pn.__version__) < Version("1") or Version(hvplot.__version__) < Version(
            "0.8.1"
        )
    except ImportError:
        error = True

    if error:
        raise RuntimeError(
            "Please install panel and hvplot to use the GUI\n"
            "`conda install -c conda-forge 'panel>=1' 'hvplot>=0.8.1'`"
        )

    from .gui import GUI

    css = """
    .scrolling {
      overflow: scroll;
    }
    """
    pn.config.raw_css.append(css)  # add scrolling class from css (panel GH#383, GH#384)
    ex = pn.extension("codeeditor", template="fast")
    gl["instance"] = GUI()
    return ex


def __getattr__(attr):
    if attr in {"instance", "gui"}:
        do_import()
    return gl[attr]


def output_notebook(*_, **__):
    """
    Load the notebook extension
    """
    return do_import()
