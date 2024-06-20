<<<<<<< Updated upstream
DEFAULT_BRANCH=dev
# Assume this file is in the reporoot/scripts directory
reporoot=$(dirname $0)/..
cd $reporoot

# Make sure we're starting from the base branch
# git checkout $DEFAULT_BRANCH 
=======
debug=echo
# Assume this file is in the reporoot/scripts directory
reporoot=$(dirname $0)/..
cd $reporoot

# Make sure required env vars are set
if [ -z "$DPK_DOCKER_REGISTRY_USER" ]; then
    echo DPK_DOCKER_REGISTRY_USER env var must be set
    exit 1
elif [ -z "$DPK_DOCKER_REGISTRY_KEY" ]; then
    echo DPK_DOCKER_REGISTRY_KEY env var must be set
    exit 1
elif [ -z "$DPK_PYPI_USER" ]; then
    echo DPK_PYPI_USER env var must be set
    exit 1
elif [ -z "$DPK_PYPI_TOKEN" ]; then
    echo DPK_DPYP_TOKEN env var must be set
    exit 1
fi

if [ -z "$debug" ]; then
    DEFAULT_BRANCH=dev
else
    DEFAULT_BRANCH=releasing
fi

# Make sure we're starting from the base branch
get fetch
git checkout $DEFAULT_BRANCH 
>>>>>>> Stashed changes

# Get the currently defined version w/o any suffix.  This is the next release version
# version=$(make DPK_VERSION_SUFFIX= show-version)
# tag=v$version

# Create a new branch for this version and switch to it
# git branch -r release/$tag

# Remove the release suffix in this branch
# cat .make.versions | sed -e 's/^DPK_VERSION_SUFFIX.*/DPK_VERSION_SUFFIX=/' > tt
# mv tt .make.version

# Apply the unsuffixed version to the repo and check it into this release branch
<<<<<<< Updated upstream
# make set-versions
# git add -A
# git commit -s -m "Cut release $version"
# git push origin
# git tag -a -s -m "Cut release $version" $tag 
# git push origin $tag 
=======
 make set-versions
git add -A
git commit -s -m "Cut release $version"
git push origin
git tag -a -s -m "Cut release $version" $tag 
git push --set-upstream origin release/$tag
git push origin $tag 
>>>>>>> Stashed changes

# Now build with the updated version
# Requires quay credentials in the environment!
# DPL_DOCKER_REGISTRY_USER=dataprep1
# DPK_DOCKER_REGISTRY_KEY=...
# Requires pypi credentials in the environment!
# DPK_PYPI_USER=__token__
# DPK_PYPI_TOKEN=...
# make -C transforms/noop DPK_VERSION_SUFFIX=.dev7 build publish
# make build publish

# Now go back to the default branch so we can bump the minor version number and reset the version suffix
<<<<<<< Updated upstream
# git branch $DEFAULT_BRANCH
=======
git checkout $DEFAULT_BRANCH
>>>>>>> Stashed changes

# Change to the next development version (bumped minor version with suffix).
# Do we want to control major vs minor bump
minor=$(cat .make.versions | grep '^DPK_MINOR_VERSION=' | sed -e 's/DPK_MINOR_VERSION=\([0-9]*\).*/\1/') 
minor=$(($minor + 1))
#cat .make.versions | sed -e "s/^DPK_MINOR_VERSION=.*/DPK_MINOR_VERSION=$minor/"  \
# 			  -e "s/^DPK_VERSION_SUFFIX=.*/DPK_VERSION_SUFFIX=.dev0/"  > tt
#mv tt .make.versions

# Push the version change back to the origin
# git add -A
# git commit -s -m "Bump minor version to $minor after cutting release $version"
# git push origin
