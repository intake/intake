#!/bin/bash

mkdir -p $PREFIX/share/intake/civilservices
cp $SRC_DIR/data/states.csv $PREFIX/share/intake/civilservices
cp $RECIPE_DIR/us_states.yaml $PREFIX/share/intake/
