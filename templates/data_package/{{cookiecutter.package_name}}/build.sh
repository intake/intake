#!/bin/bash
INTAKE_CATALOG_DIR=$PREFIX/share/intake/
mkdir -p INTAKE_CATALOG_DIR

{% if cookiecutter.install_local_data_files == 'yes' %}
DATA_DIR=$INTAKE_CATALOG_DIR/{{cookiecutter.dataset_name}}
mkdir -p $DATA_DIR
cp -a $RECIPE_DIR/src/ $DATA_DIR/
{% endif %}

cp $RECIPE_DIR/{{cookiecutter.dataset_name}}.yaml $INTAKE_CATALOG_DIR
