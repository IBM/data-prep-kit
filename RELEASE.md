# Release Management

## Overview 
Releases are created from the main repository branch using the version
numbers, including an intermediate version suffix, 
defined in `.make.versions`.
The following points are important:

1. In general, a common version number is used for all published pypi wheels and docker images.
1. `.make.versions` contains the version to be used when publishing the **next** release. 
1. Whenever `.make.versions` is changed, `make set-versions` should be run from the top of the repo.
   1. Corollary: `make set-versions` should ONLY be used from the top of the repo when `.make.versions` changes.
1. The main branch always has the version suffix set to .dev\<N\>, which
allows intermediate publishing from the main branch using version X.Y.Z.dev\<N\>.
1. The `scripts/release-branch.sh` script automates creation of a new release branch and tag and version numbers in `.make.versions` 
1. Building and publishing is done manually, or soon via a git action, in the branch created by `scripts/release-branch.sh`. 
   1. Wheels can only be published once to pypi for a given version.
   1. Transform and kfp images may be republished to the docker registry.
1. Releases done via the `release-branch.sh` script will have their micro version number set to 0 (e.g., 1.2.0)
1. Intermediate releases that bump the micro version may be done by individual transforms. This can mean
that version X.Y.Z of a transform is equivalent to the X.Y+1.0 release.  The latter created when running
the `release-branch.sh` script.
   
## Cutting the release
Creating the release involves

1. Editing the `release-notes.md` to list major/minor changes and commit to the main branch.
1. Creating a release branch and updating the main branch versions (using `release-branch.sh`).
1. Creating a github release and tag from the release branch using the github web UI.
1. Building and publishing pypi library wheels and docker registry image.

Each is discussed below.

### Editing release-notes.md 
Make a dummy release on github (see below) to get a listing of all commits.
Use this to come up with the items.
Commit this to the main branch so it is ready for including in the release branch.

### Creating release branch 
The `scripts/release-branch.sh` is currently run manually to create the branch and tags as follows:

1. Creates the `releases/vX.Y.Z` from the main branch where `X.Y.Z` are defined in .make.versions
1. Creates the `pending-releases/vX.Y.Z` branch for PR'ing back into the `releases/vX.Y.Z` branch. 
1. In the new `pending-releases/vX.Y.Z` branch 
    1. Nulls out the version suffix in the new branch's `.make.version` file. 
    1. Applies the unsuffixed versions to the artifacts published from the repo using `make set-versions`..
    1. Commits and pushes branch 
1. Creates the `pending-version-change/vX.Y.Z` branch for PR'ing back into the main branch.
    * Note: this branch is named with the new release version (i.e. vX.Y.Z), however
      the version in this branch is actually X.Y+1.0.dev0.
1. In the `pending-version-change/vX.Y.Z` branch
    1. Increments the minor version (i.e. Z+1) and resets the suffix to `dev0` in `.make.versions`.
    1. Commits and pushes branch 

To double-check the version that will be published from the release,
```
git checkout pending-releases/vX.Y.Z 
make show-version
```
This will print for example, 1.2.3. 

To run the script from the top of the repo:

```shell
scripts/release-branch.sh
```

After running the script, you should
1. Create a pull request from branch `pending-releases/vX.Y.Z` into the `releases/vX.Y.Z` branch, and merge.
2. Use the github web UI to create a git release and tag of the `releases/vX.Y.Z` branch
3. Create a pull request from branch `pending-version-change/vX.Y.Z` into the main branch, and merge. 

### Creating the Github Release
After running the `release-branch.sh` script, to create tag `vX.Y.Z` and branch `releases/vX.Y.Z`
and PRing/merging `vX.Y.Z` into `releases/vX.Y.Z`.
1. Go to the [releases page](https://github.com/IBM/data-prep-kit/releases). 
1. Select `Draft a new release`
1. Select target branch `releases/vX.Y.Z`
1. Select `Choose a tag`, type in vX.Y.Z, click `Create tag`
1. Press `Generate release notes` 
1. Add a title (e.g., Release X.Y.Z) 
1. Add any additional relese notes.
1. Press `Publish release`

### Building and Publishing Wheels and Images
After creating the release and tag on github: 

1. Switch to a release branch (e.g. releases/v1.2.3). 
1. Be sure you're at the top of the repository (`.../data-prep-kit`)
1. Optionally, `make show-version` to see the version that will be published
1. Running the following, either manually or in a git action
    1. `make build`
    1. `make publish`	(See credential requirements below)

For docker registry publishing, the following environment variables/credentials are needed:

* DPK_DOCKER_REGISTRY_USER - user used with the registry defined in DOCKER_HOST in `.make.defaults`
* DPK_DOCKER_REGISTRY_KEY - key/password for docker registry user.

To publish to pypi, the credentials in `~/.pypirc` file (let us know if there is a way to do
this with environment variables).
See [pypi](https://packaging.python.org/en/latest/specifications/pypirc/) for details.


 

