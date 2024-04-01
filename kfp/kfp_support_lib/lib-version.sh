#!/usr/bin/env bash

set -x

# return ok if the lib version is new.

LIB_VERSION=$(cat pyproject.toml | grep version | sed 's/version = //' | sed 's|[",)(]||g')
LIB_NAME=$(cat pyproject.toml | grep -E "^name" | sed 's/name = //' | sed 's|[",)(]||g')
VERSIONS="$(pip index --python-version=3.10 versions ${LIB_NAME} -i https://test.pypi.org/simple/ | grep "Available versions:" | sed 's|["Available versions:"\,]| |g')"

result="ok"
for version in $VERSIONS; do     
	if [[ ${LIB_VERSION} == $version ]]; then
		echo "version ${LIB_VERSION} already exists"
		result="notok"
		break
	fi
done
echo "$result"
