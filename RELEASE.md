# Release Management

## Overview 
Release are created from the main repository branch using the version
numbers, including an intermediate version suffix, 
defined in `.make.versions`.
The following points are important:

1. In general, common a version number is used for all published pypi wheels and docker images.
1. `.make.versions` contains the version to be used when publishing the **next** release. 
1. Whenever `.make.versions` is changed, `make set-versions` should be run from the top of the repo.
   1. Corollary: `make set-versions` should ONLY be used from the top of the repo when `.make.versions` changes.
1. The main branch always has the version suffix set to .dev\<N\>, which
allows intermediate publishing from the main branch using version X.Y.Z.dev\<N\>.
1. The `scripts/release.sh` script automates the following:
   1. Creating a `releases/vX.Y.Z` branch and `vX.Y.Z` tag.
   2. Nulling out the version suffix in the new branch's `.make.version` file. 
   3. Applying the unsuffixed versions to the artifacts published from the repo.
   4. Incrementing the minor version and resetting the suffix in the main branch.
1. Building and publishing is done manually, or soon via a git action, in the branch created by `scripts/release.sh`. 
   1. Wheels can only be published once to pypi for a given version.
   1. Transform and kfp images may be republished to the docker registry.
   
## Cutting the release
Creating the release involves

1. Creating a release branch and tag.
1. Building and publish pypi library wheels and docker registry image.
1. Creating a github release from the release branch and tag.

Each is discussed below

### Creating release branch and tag 
The `scripts/release.sh` is currently run manually to create the branch and tags 
for the next release using versions in `.make.versions`.
The script performs the following:

1. Creates the `release/vX.Y.Z` and tag `vX.Y.Z` where `X.Y.Z` are defined in .make.versions 
1. Resets main branch to version `X.Y.Z+1` with version suffix `.dev0`. 

To double-check the version that will be published from the main branch,
```
git checkout <main branch>
make DPK_VERSION_SUFFIX= show-version
```
This will print for example, 1.2.3. 

To run the script from the top of the repo:

```shell
scripts/release.sh
```

### Publishing wheels and images
After creating the release branch and tag using the `scripts/release.sh` script:

1. Switch to a release branch (e.g. releases/v1.2.3) created by the `release.sh` script
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


### Github release
After running the `release.sh` script, to create tag `vX.Y.Z` and branch `releases/vX.Y.Z`
1. Go to the [releases page](https://github.com/IBM/data-prep-kit/releases). 
2. Select `Draft a new release`
3. Select `Choose a tag -> vX.Y.Z`
4. Press `Generate release notes` 
5. Add a title (e.g., Release X.Y.Z) 
6. Add any additional relese notes.
7. Press `Publish release`
 

