# Set REPOROOÞ
REPO_ROOT=$(cd ../../../ && pwd && cd - > /dev/null )
echo $REPO_ROOT


. ./venv/bin/activate
jupyter notebook --no-browser demo_with_launcher.ipynb
