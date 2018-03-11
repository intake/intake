#!/bin/bash

mkdir -p $PREFIX/share/intake/us_crime
cp $RECIPE_DIR/data/crime.csv $PREFIX/share/intake/us_crime
cp $RECIPE_DIR/us_crime.yaml $PREFIX/share/intake/
