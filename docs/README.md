# Building Documentation

An environment with several prerequisites is needed to build the
documentation.  Create this with:

```bash
conda create -n intake python=3.6 pandas dask python-snappy appdirs -c conda-forge -y
conda activate instak
```

Additional pip packages are listed in `./requirements.txt` are required to
build the docs:

```bash
pip install -r requirements.txt
```

To make HTML documentation:

```bash
make html
```

Outputs to `build/html/index.html`
