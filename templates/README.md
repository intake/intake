# Cookiecutter Templates

This directory contains
[Cookiecutter](https://cookiecutter.readthedocs.io/en/latest/) templates for
making new Intake plugins and data packages.

To use these templates, install cookiecutter:
```
conda install -c defaults -c conda-forge cookiecutter
```
or
```
pip install cookiecutter
```

For a new plugin:
```
cookiecutter gh:intake/intake/templates/plugin
```

And for a new conda data package:
```
cookiecutter gh:intake/intake/templates/data_package
```

The template will prompt for parameters.
