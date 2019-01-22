function install() {
    conda config --set auto_update_conda off
    conda config --set always_yes yes
    conda config --add channels conda-forge
    conda config --get channels
    conda update -q conda
    conda install jinja2 pyyaml pytest conda-build
    conda install $(python scripts/deps.py)
}

install