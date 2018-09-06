try:
    import ipywidgets
    from .widgets import DataBrowser

except ImportError:

    class DataBrowser(object):
        def __repr__(self):
            raise RuntimeError("Please install ipywidgets to use the Data "
                               "Browser")
