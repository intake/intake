#!/bin/bash

set -e # exit on error

CONDA_REQS="conda-build=3.17.0 anaconda-client=1.7.2 conda-verify=3.1.1"

PLATFORM="$(case $TRAVIS_OS_NAME in (linux) echo Linux;; (osx) echo MacOSX;;esac)"

echo "Installing Miniconda."
MINICONDA_URL="https://repo.continuum.io/miniconda"
MINICONDA_FILENAME=Miniconda3-latest-${PLATFORM}-x86_64.sh
curl -L -O "${MINICONDA_URL}/${MINICONDA_FILENAME}"
bash ${MINICONDA_FILENAME} -b

# if emergency, temporary package pins are necessary, they can go here
PINNED_PKGS=$(cat <<EOF
EOF
)
mkdir -p $HOME/miniconda3/conda-meta
echo -e "$PINNED_PKGS" > $HOME/miniconda3/conda-meta/pinned

echo "Configuring conda."
conda config --set auto_update_conda off
conda install --yes ${CONDA_REQS}

echo "Installing test dependencies."
conda install --yes `python scripts/deps.py`
