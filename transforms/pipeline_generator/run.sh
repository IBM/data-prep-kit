#!/bin/bash

# TODO: implement this logic in Makefile

DIST_DIR=$1
ROOT_DIR=${PWD}/../..
VENV_ACTIVATE=${ROOT_DIR}/env/bin/activate

source $VENV_ACTIVATE
mkdir -p ${ROOT_DIR}/${DIST_DIR}/
python3 pipeline_generator.py -c code_quality/pipeline_definitions.yaml -od ${ROOT_DIR}/${DIST_DIR}/
