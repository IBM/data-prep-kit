#!/bin/bash
# Looks in files to make sure there is some form of comment with license text
echo -n Checking for missing license text...
# Script is having trouble dealing with names with white space, so exclude the 'fine tuning' directory
#files2check=$(find . -name '*.py' -o -name '*.sh' -o -name '*.ipynb' | grep -v venv | grep -v __init__  | grep -v 'fine tuning')
# And, for now only check .py files that are not in the examples tree since there are lots otherwise. 
files2check=$(find . -name '*.py' | grep -v venv | grep -v __init__  | grep -v examples | grep -v 'fine tuning')
files=
for file in $files2check; do
   license=$(cat "$file" | grep '^#.*[lL]icense')	
   auto=$(cat "$file" | grep '^#.*auto')	# Ignore auto-generated files
   if [ -z "$license" -a -z "$auto" ]; then
	if [ -z "$files" ]; then
	    echo the following appear to be missing license text.
	fi
	echo $file
	files="$files $file"
   fi
done
if [ ! -z "$files" ]; then
    echo "To address this, add a comment header with license text (including the word 'license')."
    status=1
else
    echo no files appear to be missing license text.
    status=0
fi
exit $status
