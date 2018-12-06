#!/bin/bash

set -e # exit on error

source ${HOME}/miniconda3/bin/activate root

# Workaround for Travis-CI bug #2: https://github.com/travis-ci/travis-ci/issues/7773
if ! [ "$TRAVIS_OS_NAME" = "linux" ]; then
    EXTRA_OPTS="--no-test"
fi

echo "Building conda package."
conda build -c defaults -c conda-forge ${EXTRA_OPTS:-} ./conda

# If tagged, upload package to main channel
if [ -n "$TRAVIS_TAG" ]; then
    echo "Uploading conda package."
    anaconda -t ${ANACONDA_TOKEN} upload -u intake --force `conda build --output ./conda`
fi
