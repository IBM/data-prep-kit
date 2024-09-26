#!/bin/bash
# Check that each transform in transforms/<category>/<transform> has a corresponding 
# .github/workflows/test-<category>-<transforms>.yml file.
if [ ! -d transforms ]; then
    echo Please run this script from the top of the repository
    exit 1
fi
for i in $(find transforms  -maxdepth 2 -mindepth 2 -type d | grep -v venv); do 
    transform=$(basename $i)
    category=$(dirname $i)
    category=$(basename $category)
    workflow=.github/workflows/test-$category-$transform.yml
    if [ ! -e $workflow ]; then 
	echo Missing $workflow for transform $category/$transform 
	echo Fix this by running make in the .github/workflows directory
	exit 1
    else
	echo Verified existence of $workflow
    fi 
done
