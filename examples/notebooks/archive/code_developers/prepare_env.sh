

# Set REPOROOT
REPO_ROOT=$(cd ../../../ && pwd && cd - > /dev/null )
echo $REPO_ROOT

pip install jupyter 

requirement_files=(
"$REPO_ROOT/tools/ingest2parquet/requirements.txt"
)

# Iterate through the list and install requirements from each file
for requirements_file in "${requirement_files[@]}"
do
  echo "Install packages from $requirements_file"
  pip install -r "$requirements_file"
done


pip install duckdb pandas parameterized scipy transformers
pip install "networkx==3.3" "colorlog==6.8.2" "func-timeout==4.3.5" "pandas==2.2.2" "emerge-viz==2.0.0"
