# Release Process

The process of creating a release is described in this document.
Note: below we list the manual steps to create a new release, later some of them will be automated by GitHub actions.

Replace `X.Y.Z` with the version to be released.

## 1. Update the .make.versions file

**Question:** should we use the same versions for release, and for shared python modules and images, such as 
`DPK_LIB_VERSION`, `DPK_LIB_KFP_VERSION`, `DPK_LIB_KFP_VERSION_v2`, `DPK_LIB_KFP_SHARED`, `KFP_DOCKER_VERSION`, `KFP_DOCKER_VERSION_v2`?
If YES, maybe we can combine all these variables into a single one.

- In the [.make.versions](.make.versions) file update versions of the shared components `DPK_LIB_VERSION`, 
`DPK_LIB_KFP_VERSION`, `DPK_LIB_KFP_VERSION_v2`, `DPK_LIB_KFP_SHARED`, `KFP_DOCKER_VERSION`, `KFP_DOCKER_VERSION_v2` to the desired ones  
- Update transformers versions
- Set `RELEASE_VERSION_SUFFIX` to the empty string, or comment out the entry.

## 2. Run local tests
TODO
## 3. Create a `releases/X.Y.Z` branch
The `releases/X.Y.Z` branch should be created from a base branch. 

For major and minor releases the base is `master` and for patch releases (fixes) the base is `releases/X.Y.(Z-1)`.
You can do that [directly from GitHub](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-and-deleting-branches-within-your-repository#creating-a-branch).

## 4. Create a [new release](https://github.com/fybrik/fybrik/releases/new)

Use `vX.Y.Z` tag and set `releases/X.Y.Z` as the target.

Ensure that the release notes explicitly mention upgrade instructions and any breaking change.

## 5. Publish the data-processing-lib Python modules

From the project root directory execute
```shell
make -C data-processing-lib publish
```
Provide relevant [PyPi](https://pypi.org/) username and password (token) when it will be asked.

## 6. Publish KFP support Python modules and KFP Components Container images

From the project root directory execute
```shell
make -C kfp publish
```
Provide relevant [PyPi](https://pypi.org/) and [quay](https://quay.io/) username and password (token) when it will be asked.

_Note:_ The step should be executed twice, when the environment variable `KFPv2` is not set and when it is equals to "1"

## 7. Publish transformers images

From the project root directory execute
```shell
make -C transformers publish
```
## 8. Prepare the .make.versions file for the next release

Set  `RELEASE_VERSION_SUFFIX` to "dev1"
**Note:** better to set the next release versions, so the images/modules will be X.Y.Z+1.dev1, but we don't know if 
the next release will be major or minor release.
