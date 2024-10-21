#!/bin/bash
usage() {
cat << EOF
Check that each transform in transforms/<category>/<transform> has a corresponding 
  .github/workflows/test-<category>-<transforms>.yml file and,
  .github/workflows/test-<category>-<transforms>-kfp.yml file if 
	there is a kfp_ray directory for the transform, and
	the transform is not in the kfp black list.
Options:
   -show-kfp-black-list: prints the space separate list of transform 
	directories (base names) and exits.
   -help: show this message.
EOF
}

if [ ! -d transforms ]; then
    echo Please run this script from the top of the repository
    exit 1
fi
KFP_BLACK_LIST="doc_chunk pdf2parquet pii_redactor text_encoder license_select repo_level_ordering"
while [ $# -ne 0 ]; do
   case $1 in
        -show-kfp-black-list)    echo $KFP_BLACK_LIST; exit 0;
	;;
        *help)          	usage; exit 0;  
	;;
	*) echo Unrecognized option $1. exit 1
	;;
   esac
   shift; 
done
for i in $(find transforms  -maxdepth 2 -mindepth 2 -type d | grep -v venv); do 
    transform=$(basename $i)
    category=$(dirname $i)
    category=$(basename $category)
    workflows=.github/workflows/test-$category-$transform.yml
    is_blacklisted=$(echo $KFP_BLACK_LIST | grep $transform)     
    if [ -d $i/kfp_ray -a -z "$is_blacklisted" ]; then
    	workflows="$workflows .github/workflows/test-$category-$transform-kfp.yml"
    else
	echo KFP workflow for $transform is not expected. 
    fi
    for workflow in $workflows; do
	if [ ! -e $workflow ]; then 
	    echo Missing $workflow for transform $category/$transform 
	    echo Fix this by running make in the .github/workflows directory
	    exit 1
	else
	    echo Verified existence of $workflow
	fi 
    done
done
