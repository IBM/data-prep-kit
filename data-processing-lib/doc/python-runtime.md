## Python Runtime
The python runtime provides a simple mechanism to run a transform on a set of input data to produce
a set of output data, all within the python execution environment.

A `PythonTransformLauncher` class is provided that enables the running of the transform.  For example,

```python
launcher = PythonTransformLauncher(YourTransformConfiguration())
launcher.launch()
```
The `YourTransformConfiguration` class configures your transform.
More details can be found in the [transform tutorial](transform-tutorials.md).