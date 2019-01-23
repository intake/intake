function build(){
    conda build -c defaults -c conda-forge --no-test ./conda
    conda install -y --use-local intake
    conda list
}

build