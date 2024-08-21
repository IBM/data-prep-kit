# Document ID Annotator

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Building

A [docker file](Dockerfile) that can be used for building docker image. You can use

```shell
make build 
```

## Driver options

## Configuration and command line Options

See [Python documentation](../python/README.md)

## Running

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following [command line arguments](../python/README.md) are available in addition to 
[the options provided by the ray launcher](../../../../data-processing-lib/doc/ray-launcher-options.md).

To use the transform image to transform your data, please refer to the
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
