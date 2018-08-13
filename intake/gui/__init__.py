try:
    import ipywidgets
    from .widgets import DataBrowser

except ImportError:

    class DataBrowser(object):
        def __getattribute__(self, item):
            raise RuntimeError("Please install ipywidgets to use the Data "
                               "Browser")
