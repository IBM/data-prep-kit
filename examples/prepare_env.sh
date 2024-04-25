

# Set REPOROOT
REPO_ROOT=$(cd ../ && pwd && cd - > /dev/null )
echo $REPO_ROOT

pip install jupyter 

requirement_files=(
"$REPO_ROOT/transforms/code/malware/requirements.txt"
"$REPO_ROOT/transforms/code/code_quality/requirements.txt"
"$REPO_ROOT/transforms/code/proglang_select/requirements.txt"
"$REPO_ROOT/transforms/universal/ededup/requirements.txt"
"$REPO_ROOT/transforms/universal/fdedup/requirements.txt"
"$REPO_ROOT/transforms/universal/filter/requirements.txt"
"$REPO_ROOT/tools/ingest2parquet/requirements.txt"
"$REPO_ROOT/transforms/universal/doc_id/requirements.txt"
"$REPO_ROOT/transforms/universal/tokenization/requirements.txt"
)

# Iterate through the list and install requirements from each file
for requirements_file in "${requirement_files[@]}"
do
  echo "Install packages from $requirements_file"
  pip install -r "$requirements_file"
done
