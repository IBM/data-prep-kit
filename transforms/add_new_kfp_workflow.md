# Adding new KFP workflows

This README outlines the steps to add a new KFP workflow for a new transform under [transforms](./transforms/) directory.

1) Create a new `kfp_ray` directory in the transform directory, similar to [this directory](universal/noop/kfp_ray/).

2) Create the workflow and add it to `kfp_ray` directory. It is recommended to use the [pipeline generator](../kfp/pipeline_generator/single-pipeline/) for that. If the workflow was generated using the [pipeline generator](../kfp//pipeline_generator/single-pipeline/) also include `pipeline_definitions.yaml` file used to generate the workflow in the `kfp_ray` directory.

3) Add `Makefile` file to `kfp_ray` directory similar to [this Makefile example](./universal/noop/kfp_ray/Makefile).

3) Add the path to the transform input directory in the [populate_minio script](../scripts/k8s-setup/populate_minio.sh). This path is used when testing the workflow.
4) Create a GitHub Action for the kfp workflow using the `make` command in the [.github/workflows/](../.github/workflows/README.md) directory.
5) Update the workflows list in [README.md](../kfp/README.md) file.
