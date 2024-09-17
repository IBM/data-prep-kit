# Workflow Management

Here we have the start of a sstem to automatically generated workflows. 
In general, the design is to use template ans make to generate/update the workflows.

```
make
git commit -a -s -m "update workflows"
git push
```

should be sufficient.

## Transforms 
For transforms, we define a unique test workflow for each transform, based on a template [test-transform.template](test-transform.template).
The Makefile is used to (re)generate all workflows a necessary.  By design, workflows for a given transform should run when

* anything of substance effecting operation is modified in the transform's directory tree.
* anything in the core libraries in this repo supporting the transform change (e.g., data-processing/lib).
* Help! the workflow should NOT run when documentation (e.g., !**.md) is changed, however disabling this case does not seem to be working atm.

When a new transform is added to the repository, 

1. add it to the corresponding macro in the Makefile 
1. Run `make` to create the new test .yml for the transform
1. commit and push the change.

