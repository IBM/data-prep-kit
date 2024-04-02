#!/bin/bash

DIST_DIR=$1
ROOT_DIR=${PWD}/..
VENV_ACTIVATE=${ROOT_DIR}/../kfp_support_lib/venv/bin/activate

source $VENV_ACTIVATE
mkdir -p ${ROOT_DIR}/${DIST_DIR}/
python3 pipeline_generator.py -c example/pipeline_definitions.yaml -od ${ROOT_DIR}/${DIST_DIR}/
