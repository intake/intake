# Building Documentation

An environment with several prerequisites is needed to build the
documentation.  Create this with:

## First option for environment

```bash
conda create -n intake-docs python=3.8 pandas dask python-snappy appdirs -c conda-forge -y
conda activate intake-docs
```

Additional pip packages are listed in `./requirements.txt` are required to
build the docs:

```bash
pip install -r requirements.txt
```

## Second option for environment

A conda environment with pip packages included is in `environment.yml` of the current directory, and you may create it with:

```bash
conda env create
conda activate intake-docs
```

## Build docs

To make HTML documentation:

```bash
make html
```

Outputs to `build/html/index.html`
