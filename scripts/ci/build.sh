#!/bin/bash

set -e # exit on error

echo "Building conda package."
conda build -c defaults -c conda-forge --no-test ./conda

# If tagged, upload package to main channel, otherwise, run tests
if [ -n "$TRAVIS_TAG" ]; then
    echo "Uploading conda package."
    anaconda -t ${ANACONDA_TOKEN} upload -u intake --force `conda build --output ./conda`
else
    echo "Installing conda package locally."
    conda install -y --use-local intake

    echo "Running unit tests."
    py.test --cov=intake
fi
