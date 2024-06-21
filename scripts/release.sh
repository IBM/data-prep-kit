debug=echo
dbg_suffix=.dev7
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
fi
if [ ! -e ~/.pypirc ]; then
   cat << EOF
You need a ~/.pypirc containing pypi.org credentials.
See https://packaging.python.org/en/latest/specifications/pypirc/ for details.
EOF
    exit
fi
exit

if [ -z "$debug" ]; then
    DEFAULT_BRANCH=dev
else
    DEFAULT_BRANCH=releasing-copy
fi

# Make sure we're starting from the base branch
get fetch
git checkout $DEFAULT_BRANCH 

# Get the currently defined version w/o any suffix.  This is the next release version
version=$(make DPK_VERSION_SUFFIX= show-version)

if [ -z "$debug" ]; then
    tag=v$version
else
    tag=test$version
fi

# Create a new branch for this version and switch to it
release_branch=releases/$tag
if [ ! -z "$debug" ]; then
    # delete local tag and branch
    git tag --delete $tag
    git branch --delete $release_branch
    # delete remote tag and branch
    git push --delete origin $tag
    git push --delete origin $release_branch
fi
git checkout -b $release_branch 


# Remove the release suffix in this branch
# Apply the unsuffixed version to the repo and check it into this release branch
if [ -z "$debug" ]; then
    cat .make.versions | sed -e 's/^DPK_VERSION_SUFFIX.*/DPK_VERSION_SUFFIX=/' > tt
    mv tt .make.versions
else
    cat .make.versions | sed -e "s/^DPK_VERSION_SUFFIX.*/DPK_VERSION_SUFFIX=$dbg_suffix/" > tt
    mv tt .make.versions
fi
# Apply the version change to all files in the repo 
make set-versions

# Commit the changes to the release branch and tag it
git status
git commit -s -a -m "Cut release $version"
git push --set-upstream origin $release_branch 
git tag -a -s -m "Cut release $version" $tag 
git push origin $tag 

# Now build with the updated version
# Requires quay credentials in the environment, DPL_DOCKER_REGISTRY_USER, DPK_DOCKER_REGISTRY_KEY
if [ -z "$debug" ]; then
    make build publish
else
    # make -C data-processing-lib/spark image # Build the base image required by spark
    make -C transforms/universal/noop/python build publish
fi

# Now go back to the default branch so we can bump the minor version number and reset the version suffix
git checkout $DEFAULT_BRANCH

# Change to the next development version (bumped minor version with suffix).
micro=$(cat .make.versions | grep '^DPK_MICRO_VERSION=' | sed -e 's/DPK_MICRO_VERSION=\([0-9]*\).*/\1/') 
micro=$(($micro + 1))
cat .make.versions | sed -e "s/^DPK_MICRO_VERSION=.*/DPK_MICRO_VERSION=$micro/"  \
 			 -e "s/^DPK_VERSION_SUFFIX=.*/DPK_VERSION_SUFFIX=.dev0/"  > tt
mv tt .make.versions
# Apply the version change to all files in the repo 
make set-versions

# Push the version change back to the origin
next_version=$(make show-version)
git commit -s -a -m "Bump micro version to $next_version after cutting release $version into branch $release_branch"
git diff origin/$DEFAULT_BRANCH $DEFAULT_BRANCH
if [ -z "$debug" ]; then
    git push origin
fi
