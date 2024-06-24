# Release Management

Release are created from the main repository branch using the version
numbers, including an intermediate version suffix, 
defined in `.make.versions`.
The following points are important:

1. `.make.versions` contains the version to be used when publishing the **next** release. 
1. The main branch always has the version suffix set to .dev<N>, which
allows intermediate publishing from the dev branch using version X.Y.Z.dev<N>.
2. In general, common version number is used for all published pypi wheels and docker images.
3. The `scripts/release.sh` script automates the following:
   1. Creating a `release/vX.Y.Z` branch and `vX.Y.Z` tag
   2. Nulling out the version suffix in the new branch's `.make.version` file. 
   3. Applying the unsuffixed versions to the artifacts published from the repo.
   4. Building and publishing the wheels to pypi and images to a docker registry. 
   5. Incrementing the minor version and resetting the suffix in the main branch.
   
# Cutting the release
Creating the release requires running the `release.sh` script and optionally
generating a release on github.  The latter can be performed manually
once the `release.sh` script has done its work.

## release.sh
Running `release.sh` requires credentials to publish to the various cloud locations.

For docker registry publishing, the following environment variables/credentials are needed:

* DPK_DOCKER_REGISTRY_USER - user used with the registry defined in DOCKER_HOST in `.make.defaults`
* DPK_DOCKER_REGISTRY_KEY - key/password for docker registry user.

To publish to pypi, the credentials in `~/.pypirc` file (let us know if there is a way to do
this with environment variables).
See [pypi](https://packaging.python.org/en/latest/specifications/pypirc/) for details.

To see the version that will be published,
```
make DPK_VERSION_SUFFIX= show-version
```
This will print for example, 1.2.3. 

To generate the release :
```shell
bash scripts/release.sh
```

## Github release
After running the `release.sh` script, to create tag `vX.Y.Z` and branch `releases/vX.Y.Z`
1. Go to the [releases page](https://github.com/IBM/data-prep-kit/releases). 
2. Select `Draft a new release`
3. Select `Choose a tag -> vX.Y.Z`
4. Press `Generate release notes` 
5. Add a title (e.g., Release X.Y.Z) 
6. Add any additional relese notes.
7. Press `Publish release`
 

