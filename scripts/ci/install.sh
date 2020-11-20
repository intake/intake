#!/bin/bash

set -e # exit on error
wget "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O miniconda.sh
bash miniconda.sh -b

echo "Configuring conda."
conda config --set auto_update_conda off

echo "Installing test dependencies."
conda install --yes appdirs dask jinja2 numpy pyyaml requests msgpack-numpy pytest-cov coveralls \
    pytest fsspec intake-parquet zarr aiohttp notebook panel hvplot bokeh dask -c conda-forge -c defaults
pip install -e . --no-deps
