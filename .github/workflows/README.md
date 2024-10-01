# Workflow Management

Here we have the start of a system to automatically generated github workflows. 
In general, the design is to use templates and `make` to generate/update the workflows.

#### Goals
1. Run only tests for a given transform when only the transform changes.
Includes python, ray, spark and kfp_ray as available.  
2. When the core dpk lib components files changes, test all transforms
3. When the shared kfp components changes or core dpk lib components files changes, 
   test a randomly selected transform test. Otherwise run kfp test for the changed transforms.

#### Assumptions
1. All transforms will have test workflows.  A transform can disable its tests locally
(temporarily?) by renaming its Makefile.  For example,
`cp transforms/universal/noop/Makefile transforms/universal/noop/Makefile.disabled`.
A github action for kfp testing will not be generated if it appears in `KFP_BLACK_LIST`
in the [Makefile](./Makefile).


## DPK libraries (`data-processing-lib` directory)
The DPK libraries, in data-processing-lib/{python,ray,spark}, are tested
via the fixed 
[test-lib.yml](test-lib.yml) 
file and is triggered when any code files in that tree change.  

The transforms test workflows also depend on this directory tree and so
changes  made here will trigger transform tests.

## Transforms (`transforms` directory tree) 
We define two test workflows for each transform: one is based on a common 
template [test-transform.template](test-transform.template) and the other, for kfp testing,
is based on a common template [test-kfp-transform.template](test-kfp-transform.template).
The [Makefile](Makefile) is used to (re)generate all workflows as necessary.
By design, non kfp workflows for a given transform should run when

* anything of substance effecting operation is modified in the transform's directory tree.
* anything in the core libraries in this repo (e.g., data-processing/lib) assuming the transform depends on these.

The generated kfp workflows should run when anything of substance effecting operation is modified in the transform's directory tree 
and non of the core libraries in this repo nor the kfp components were changed. 
Otherwise a randomly chosen transform will undergo KFP testing, triggered by the [test-kfp.yml](test-kfp.yml) workflow.

When a new transform is added to the repository, 

1. Run `make` in this directory to create the new test .yml for all transforms found in transforms/{universal,code,language} directories 
1. commit and push the change to your branch with the new transform.

Something like the following:
```
git clone ....
...
git checkout -b new-branch 
make        # Creates new test*.yml workflows
git commit -a -s -m "update workflows"
git push --set-upstream origin new-branch
```

## KFP (`kfp` directory tree)

Like DPK core libs, kfp tests are defined in
[test-kfp.yml](test-kfp.yml) and run whenever changes are made in
the `kfp` directory tree as well as in the DPK core libs.  Tests currently include
test kfp on randomly selected transform.

## Miscellaneous
[test-misc.yml](test-misc.yml) defines some repo consistency tests including

1. Make sure `set-versions` make target can be run recursively throughout the repo
2. Makes sure there is a test workflow for each transform in the repo.
