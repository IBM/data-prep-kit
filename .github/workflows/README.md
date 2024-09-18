# Workflow Management

Here we have the start of a system to automatically generated github workflows (currently only for transforms). 
In general, the design is to use templates and `make` to generate/update the workflows.

Goals
1. Run only tests for a given transform when only the transform changes.
Includes python, ray, spark and kfp_ray as available.  
2. When the core dpk lib components files changes, test all transforms
3. When the shared kfp components changes, test a randomly selected transform test
   (We would like to avoid running all transform kfp tests in one PR)
4. Extra credit: If .md or other non-code changes are made, run no tests. 

Assumptions:
1. All transforms will have test workflows.  A transform can disable its tests locally
(temporarily?) by renaming its transforms/universal/noop/Makefile.disabled.
```
git clone ....
...
git checkout -b new-branch 
make                # Creates new test*.yml workflows
git commit -a -s -m "update workflows"
git push --set-upstream origin new-branch
```
should be sufficient.

## Transforms 
We define a unique test workflow for each transform, based on a common template [test-transform.template](test-transform.template).
The Makefile is used to (re)generate all workflows a necessary.  By design, workflows for a given transform should run when

* anything of substance effecting operation is modified in the transform's directory tree.
* anything in the core libraries in this repo (e.g., data-processing/lib) assuming the transform depends on these.
* Help! the workflow should NOT run when documentation (e.g., !**.md) is changed, however this case does not seem to be working atm.

When a new transform is added to the repository, 

1. Run `make` in this directory to create the new test .yml for all transforms found in transforms/{universal,code,language} directories 
1. commit and push the change to your branch with the new transform.

## KFP

## DPK libraries