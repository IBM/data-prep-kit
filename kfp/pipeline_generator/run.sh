#!/bin/bash

DEF_FILE=$1
DIST_DIR=$2
ROOT_DIR=${PWD}
VENV_ACTIVATE=${ROOT_DIR}/../kfp_support_lib/venv/bin/activate

source $VENV_ACTIVATE
mkdir -p ${ROOT_DIR}/${DIST_DIR}/
python3 pipeline_generator.py -c ${DEF_FILE} -od ${ROOT_DIR}/${DIST_DIR}/
