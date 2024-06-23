#!/usr/bin/env bash

# update ray version in dockerfile.

set -euo pipefail

if [[ $# != 1 ]]; then
  cat << EOF
        "Incorrect number of parameters provided. The required parameter is ray version.
EOF
  exit 1
fi

ray=$1

first_line=$(head -n 1 Dockerfile)
if [[ $first_line == *" AS "* ]]; then
	as_expr=$(echo $first_line | grep -Eo "AS ([a-z]+)")
	sed -i.back "s%FROM docker.io/rayproject/ray:.*%FROM docker.io/rayproject/ray:$ray\-py310 $as_expr%" Dockerfile
else
        sed -i.back "s%FROM docker.io/rayproject/ray:.*%FROM docker.io\/rayproject/ray:$ray\-py310%" Dockerfile
fi
