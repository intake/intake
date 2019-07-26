#!/bin/bash

set -e # exit on error
wget "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O miniconda.sh
bash miniconda.sh -b

echo "Configuring conda."
conda config --set auto_update_conda off

echo "Installing test dependencies."
conda install --yes appdirs dask==2 nodejs jinja2 numpy pyyaml requests msgpack-numpy coveralls \
    pytest fsspec intake-parquet zarr notebook panel==0.5.1 hvplot==0.4.0 -c defaults -c conda-forge
pip install git+https://github.com/dask/dask --upgrade --no-deps
npm install -g dat
pip install -e . --no-deps
