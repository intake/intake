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

        error = Version(pn.__version__) < Version("0.9.5") or Version(hvplot.__version__) < Version("0.8.1")
    except ImportError:
        error = True

    if error:
        raise RuntimeError("Please install panel and hvplot to use the GUI\n" "`conda install -c conda-forge 'panel>=0.9.5' 'hvplot>=0.8.1'`")

    from .gui import GUI

    css = """
    .scrolling {
      overflow: scroll;
    }
    """
    pn.config.raw_css.append(css)  # add scrolling class from css (panel GH#383, GH#384)
    pn.extension()
    gl["instance"] = GUI()


def __getattr__(attr):
    if attr in {"instance", "gui"}:
        do_import()
    return gl[attr]


def output_notebook(inline=True, logo=False):
    """
    Load the notebook extension

    Parameters
    ----------
    inline : boolean (optional)
        Whether to inline JS code or load it from a CDN
    logo : boolean (optional)
        Whether to show the logo(s)
    """
    try:
        import hvplot
    except ImportError:
        raise ImportError(
            "The intake plotting API requires hvplot." "hvplot may be installed with:\n\n" "`conda install -c pyviz hvplot` or " "`pip install hvplot`."
        )
    import holoviews as hv

    return hv.extension("bokeh", inline=inline, logo=logo)
