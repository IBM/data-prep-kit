# Adding new KFP workflows

This README outlines the steps to add a new KFP workflow for a new transform:

1) Create the workflow. [pipeline generator](./pipeline_generator/single-pipeline/) can be used for that.

2) Create a new `kfp_ray` directory in the transform directory, similar to [this directory](../transforms/universal/noop/kfp_ray/). Inside this directory, include the following:
    - The workflow file created in the previous step.
    - `pipeline_definitions.yaml`, if the workflow was generated using the [pipeline generator](./pipeline_generator/single-pipeline/).
    - `Makefile` file similar to [this Makefile example](../transforms/universal/noop/kfp_ray/Makefile).

3) Add the path to the transform input directory in the [populate_minio script](../scripts/k8s-setup/populate_minio.sh). This path is used when testing the workflow.
4) Create a GitHub Action for the kfp workflow using the `make` command in the [.github/workflows/](../.github/workflows/README.md) directory.