function install() {
    conda config --set auto_update_conda off
    conda config --set always_yes yes
    conda config --add channels conda-forge
    conda config --get channels
    conda install --yes --quiet $Env:CONDA_REQS
    conda install jinja2 pyyaml pytest conda-build
    conda install $(python scripts/deps.py)
    conda install -y -c pyviz/label/dev -c bokeh/label/dev panel bokeh hvplot
}

install
