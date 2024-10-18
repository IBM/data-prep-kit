#!/bin/bash

POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--config_file)
      DEF_FILE="$2"
      if [[ "$2" = -* ]]
      then
        echo "ERROR: config_file value not provided."
        exit 1
      fi
      shift # past argument
      shift # past value
      ;;
    -od|--output_dir_file)
      DIST_DIR="$2"
      if [[ "$2" = -* ]]
      then
        echo "ERROR: output_dir_file value not provided."
        exit 1
      fi
      shift # past argument
      shift # past value
      ;;
    -h|--help)
      echo "-c/--config_file(required): file path to config_file(pipeline_definition.yaml)."
      echo "-od/--output_dir_file(required): output folder path to store generated pipeline."
      exit 1
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done


if [ -z ${DEF_FILE+x} ]
then
echo "ERROR: config_file is not defined."
exit 1
fi

if [ -z ${DIST_DIR+x} ]
then
echo "ERROR: output_dir_file is not defined."
exit 1
fi


ROOT_DIR=${PWD}

mkdir -p ${ROOT_DIR}/${DIST_DIR}/

python3 super_pipeline_generator.py -c ${DEF_FILE} -od ${ROOT_DIR}/${DIST_DIR}/