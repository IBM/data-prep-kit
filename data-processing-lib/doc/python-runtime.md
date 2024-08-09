## Python Runtime
The python runtime provides a simple mechanism to run a transform on a set of input data to produce
a set of output data, all within a single python execution environment. We currently support two 
options of Python execution:
* Sequential execution - all files are processed sequentially
* Usage of the [Python multiprocessing pool](https://superfastpython.com/multiprocessing-pool-python/). 
In this case execution start user-defined number of processors which allows to parallelize
data processing

`Note` some of transformers, for example, exact dedup do not support multi processing Python
runtime, as they rely on a shared classes, which are not supported by this runtime

To support multiprocessing pool based runtime, Python execution introduced 2 additional 
parameters:
* `runtime_multiprocessing` flag. By default this flag is `False`, which means that sequential
execution is used. Setting it to `True` switches to multiprocessing pool runtime.
* `runtime_num_processors` defines the number of processors used by the multiprocessing pool. 
Default number of processors is 5.

Usage of these parameters allows user to choose the type of Python execution runtime and configure
parallelism in the case of multiprocessing pool.

A `PythonTransformLauncher` class is provided that enables the running of the transform.  For example,

```python
launcher = PythonTransformLauncher(YourTransformConfiguration())
launcher.launch()
```
The `YourTransformConfiguration` class configures your transform.
More details can be found in the [transform tutorial](transforms.md).