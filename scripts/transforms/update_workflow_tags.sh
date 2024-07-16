#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 3 ]]; then
  cat << EOF
	"Incorrect number of parameters provided. The required parameters are versions_file, pipeline_path and docker_image_version. 
EOF
  exit 1
fi

versions_file=$1
pipeline_path=$2
docker_image_version=$3

# Modify the tasks tags as defined in the versions file
while IFS= read -r line; do 
	[ -z "$line" ] && continue
	[[ $line == *#* ]] && continue
	[[ $line == *ifeq* || $line == *else* || $line == *endif* ]] && continue
	VERSION_NAME=$(echo $line |cut -d "=" -f 1)
	DOCKER_IMAGE_NAME=$(echo $line |cut -d "=" -f 1 |sed "s/_VERSION//" |tr '[:upper:]' '[:lower:]')
	DOCKER_IMAGE_NAME=$(echo $DOCKER_IMAGE_NAME |sed "s/_ray$/\-ray/" | sed "s/_spark$/\-spark/" | sed "s/_parquet$/\-parquet/")
	sed -i.back "s/data-prep-kit\/$DOCKER_IMAGE_NAME:.*/data-prep-kit\/$DOCKER_IMAGE_NAME:$docker_image_version\"/" $pipeline_path
done < $versions_file
