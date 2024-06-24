#!/bin/bash

POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--config_file)
      DEF_FILE="$2"
      shift # past argument
      shift # past value
      ;;
    -od|--output_dir_file)
      DIST_DIR="$2"
      shift # past argument
      shift # past value
      ;;
  esac
done

# DEF_FILE=$1
# DIST_DIR=$2
ROOT_DIR=${PWD}

mkdir -p ${ROOT_DIR}/${DIST_DIR}/
python3 -m venv venv
source venv/bin/activate
pip install pre-commit
pip install jinja2
python3 pipeline_generator.py -c ${DEF_FILE} -od ${ROOT_DIR}/${DIST_DIR}/