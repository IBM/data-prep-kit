debug=$1
dbg_suffix=.dev7
# Assume this file is in the reporoot/scripts directory
reporoot=$(dirname $0)/..
cd $reporoot

if [ -z "$debug" ]; then
    DEFAULT_BRANCH=dev
else
    DEFAULT_BRANCH=releasing
fi

# Make sure we're starting from the base branch
git fetch
git checkout $DEFAULT_BRANCH 

# Get the currently defined version w/o any suffix.  This is the next release version
version=$(make DPK_VERSION_SUFFIX= show-version)
echo Building release branch and tag for version $version

if [ -z "$debug" ]; then
    tag=v$version
else
    tag=test$version
fi
release_branch=releases/$tag
release_branch_pr=pending-releases/$tag

# Create a new branch for this version and switch to it
if [ ! -z "$debug" ]; then
    # delete local tag and branch
    git tag --delete $tag
    git branch --delete $release_branch
    # delete remote tag and branch
    git push --delete origin $tag
    git push --delete origin $release_branch
fi
echo Creating and pushing $release_branch branch to receive PR of version changes
git checkout -b $release_branch 
git push --set-upstream origin $release_branch 
git commit --no-verify -s -a -m "Target branch to receive PR for version $version"
git push 

echo Creating $release_branch_pr branch to hold version changes as the source of PR into $release_banch branch
git checkout -b $release_branch_pr
git push --set-upstream origin $release_branch_pr

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
echo Applying $version to $release_branch_pr branch 
make set-versions > /dev/null

# Commit the changes to the release pr branch 
git status
echo Committing and pushing version changes in $release_branch_pr branch. 
git commit --no-verify -s -a -m "Pull request source branch to cut release $version"
git push 

# Now create and push a new branch from which we will PR into main to update the version 
this_version=$(make show-version)
next_version_branch_pr=pending-version-change/$this_version
echo Creating $next_version_branch_pr branch for PR request back to $DEFAULT_BRANCH for version upgrade
git checkout -b $next_version_branch_pr 
git push --set-upstream origin $next_version_branch_pr
git commit --no-verify -s -a -m "Initializing branch to PR back into $DEFAULT_BRANCH holding the next development version"
git push 

# Change to the next development version (bump minor version with reset suffix).
minor=$(cat .make.versions | grep '^DPK_MINOR_VERSION' | sed -e 's/DPK_MINOR_VERSION[ ]*=[ ]*\([0-9]*\).*/\1/') 
minor=$(($minor + 1))
cat .make.versions | sed -e "s/^DPK_MINOR_VERSION=.*/DPK_MINOR_VERSION=$minor/"  	\
 			 -e "s/^DPK_MICRO_VERSION=.*/DPK_MICRO_VERSION=0/"		\
 			 -e "s/^DPK_VERSION_SUFFIX=.*/DPK_VERSION_SUFFIX=.dev0/"  > tt
mv tt .make.versions
next_version=$(make show-version)

# Apply the version change to all files in the repo 
echo Applying updated version $next_version to $next_version_branch_pr branch
make set-versions > /dev/null

# Push the version change back to the origin
if [ -z "$debug" ]; then
    echo Committing and pushing version $next_version to $next_version_branch_pr branch.
    git commit --no-verify -s -a -m "Bump version to $next_version" 
    #git diff origin/$next_version_branch_pr $next_version_branch_pr 
    git push 
else
    git status
    echo In non-debug mode, the above diffs would have been committed to the $next_version_branch_pr branch
fi

# Return to the main branch
git checkout $DEFAULT_BRANCH

cat << EOM

Summary of changes:
   1. Pushed temporary $release_branch_pr branch holding version $version of the repository. 
   2. Pushed $release_branch branch to receive PR from the $release_branch_pr branch.
   3. Pushed temporary $next_version_branch_pr branch to hold updated version $next_version of the repository on the main branch. 
   No modifications were made to the $DEFAULT_BRANCH branch.

To complete this process, please go to https://github.com/IBM/data-prep-kit and ...
   1. Create a new pull request from the $next_version_branch_pr branch back into $DEFAULT_BRANCH branch. 
   2. Create a pull request from $release_branch_pr branch into $release_branch branch.
   3. After the PR into $release_branch is merged, create a new tag $tag on $release_branch branch.
   4. Create a release from the $tag tag
   5. Once all PRs are merged, you may delete the $release_branch_pr and $next_version_branch_pr branches.
EOM
git status
