#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from distutils.version import LooseVersion

gl = globals()


def do_import():
    error = too_old = False
    try:
        import panel as pn
        too_old = LooseVersion(pn.__version__) < LooseVersion("0.9.5")
    except ImportError as e:
        error = e

    if too_old or error:
        raise RuntimeError("Please install panel to use the GUI `conda "
                           "install -c conda-forge panel>=0.8.0`. Import "
                           "failed with error: %s" % error)

    from .gui import GUI
    css = """
    .scrolling {
      overflow: scroll;
    }
    """
    pn.config.raw_css.append(css)  # add scrolling class from css (panel GH#383, GH#384)
    pn.extension()
    gl['instance'] = GUI()


def __getattr__(attr):
    if attr == 'instance':
        do_import()
    return gl['instance']


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
        raise ImportError("The intake plotting API requires hvplot."
                          "hvplot may be installed with:\n\n"
                          "`conda install -c pyviz hvplot` or "
                          "`pip install hvplot`.")
    import holoviews as hv
    return hv.extension('bokeh', inline=inline, logo=logo)
