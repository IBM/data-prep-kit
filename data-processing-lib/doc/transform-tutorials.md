# Transform Tutorials

All transforms operate on a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
read for it by the RayWorker and produce zero or more transformed tables.
The transformed tables are then written out by the RayWorker - the transform need not
worry about I/O associated with the tables.
This means the Transform itself need only be concerned with the conversion of one
in memory table at a time.

With this in mind, we start with a simple example and progress to more complex transforms:
* [Simplest transform](simplest-transform-tutorial.md)
Here we will take a simple example to show the basics of creating a simple transform
that takes a single input Table, and produces a single Table.
* [Advanced transform](advanced-transform-tutorial.md)
* [Porting from GUF 0.1.6](transform-porting.md)

Once a transform has been built, testing can be enabled with the testing framework:
* [Transform Testing](testing-transforms.md) - shows how to test a transform
independent of the ray framework.
* [End-to-End Testing](testing-e2e-transform.md) - shows how to test the
transform running in the ray environment.