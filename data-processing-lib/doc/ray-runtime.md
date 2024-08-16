# Ray Runtime 
The Ray runtime provides the ability to run in either a local or Kubernetes cluster,
and includes the following set of components:

* [RayTransformLauncher](../ray/src/data_processing_ray/runtime/ray/transform_launcher.py) - this is a 
class generally used to implement `main()` that makes use of a `TransformConfiguration` to 
start the Ray runtime and execute the transform over the specified set of input files.
The RayTransformLauncher is created using a `RayTransformConfiguration` instance.
* [RayTransformConfiguration](../ray/src/data_processing_ray/runtime/ray/runtime_configuration.py) - this 
class extends transform's base TransformConfiguration implementation to add an optional 
`TranformRuntime` (see next) class to be used by the transform implementation.
* [TransformRuntime](../ray/src/data_processing_ray/runtime/ray/transform_runtime.py) - 
this provides the ability for the transform implementor to create additional Ray resources 
and include them in the configuration used to create a transform
(see, for example, 
* [fuzzy dedup](../../transforms/universal/fdedup/ray/src/fdedup_transform_ray.py)
Many transforms will not need additional resources and can use
the [DefaultRayTransformRuntime](../ray/src/data_processing_ray/runtime/ray/transform_runtime.py).
`TransformRuntime` also provide the ability to supplement the statics collected by
[Statistics](../ray/src/data_processing_ray/runtime/ray/transform_statistics.py) (see below).

Roughly speaking the following steps are completed to establish transforms in the RayWorkers

1. Launcher parses the CLI parameters using an ArgumentParser configured with its own CLI parameters 
along with those of the Transform Configuration, 
2. Launcher passes the Transform Configuration and CLI parameters to the [RayOrchestrator](../ray/src/data_processing_ray/runtime/ray/transform_orchestrator.py)
3. RayOrchestrator creates the Transform Runtime using the Transform Configuration and its CLI parameter values
4. Transform Runtime creates transform initialization/configuration including the CLI parameters,  
and any Ray components need by the transform.
5. [RayWorker](../ray/src/data_processing_ray/runtime/ray/transform_file_processor.py) is started with configuration from the Transform Runtime.
6. RayWorker creates the Transform using the configuration provided by the Transform Runtime.
7. Statistics is used to collect the statistics submitted by the individual transform, that 
is used for building execution metadata.

![Processing Architecture](processing-architecture.jpg)

## Ray Transform Launcher
The [RayTransformLauncher](../ray/src/data_processing_ray/runtime/ray/transform_launcher.py) uses the Transform Configuration
and provides a single method, `launch()`, that kicks off the Ray environment and transform execution coordinated 
by [orchestrator](../ray/src/data_processing_ray/runtime/ray/transform_orchestrator.py).
For example,
```python
launcher = RayTransformLauncher(YourTransformConfiguration())
launcher.launch()
```
Note that the launcher defines some additional CLI parameters that are used to control the operation of the 
[orchestrator and workers](../ray/src/data_processing_ray/runtime/ray/execution_configuration.py) and 
[data access](../python/src/data_processing/data_access/data_access_factory.py).  Things such as data access configuration,
number of workers, worker resources, etc.
Discussion of these options is beyond the scope of this document 
(see [Launcher Options](ray-launcher-options.md) for a list of available options.)

## Transform Configuration
In general, a transform should be able to run in both the python and Ray runtimes.
As such we first define the python-only transform configuration, which will then
be used by the Ray-runtime-specific transform configuration. 
The python transform configuration implements  
[TransformConfiguration](../python/src/data_processing/transform/transform_configuration.py)
and defines with transform-specific name, and implementation 
and class. In addition, it is responsible for providing transform-specific
methods to define and capture optional command line arguments.
```python

class YourTransformConfiguration(TransformConfiguration):

    def __init__(self):
        super().__init__(name="YourTransform", transform_class=YourTransform)
        self.params = {}
        
    def add_input_params(self, parser: ArgumentParser) -> None:
        ...
    def apply_input_params(self, args: Namespace) -> bool:
        ...
```
Next we define the Ray-runtime specific transform configuration as an extension of
the RayTransformConfiguration and uses the `YourTransformConfiguration` above.
```python
    
class YourTransformConfiguration(RayTransformConfiguration):
    def __init__(self):
        super().__init__(YourTransformConfiguration(),
                         runtime_class=YourTransformRuntime)
```
This class provides the ability to create the instance of `YourTransformRuntime` class (see below)
as needed by the Ray runtime.  Note, that not all transforms will require a `runtime_class`
and can omit this parameter to default to an acceptable runtime class.
Details are covered in the [advanced transform tutorial](advanced-transform-tutorial.md).

## Transform Runtime
The 
[DefaultRayTransformRuntime](../ray/src/data_processing_ray/runtime/ray/transform_runtime.py)
class is provided and will be 
sufficient for many use cases, especially 1:1 table transformation.
However, some transforms will require use of the Ray environment, for example,
to create additional workers, establish a shared memory object, etc.
Of course, these transforms will generally not run outside of a Ray environment. 

```python
class DefaultRayTransformRuntime:

    def __init__(self, params: dict[str, Any]):
        ...

    def get_transform_config(
        self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        ...

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        ...
```

The RayOrchestrator initializes the instance with the CLI parameters provided by the Transform Configurations
`get_input_params()` method.

The `get_transform_config()` method is used by the RayOrchestrator to create the parameters
used to initialize the Transform in the RayWorker. 
This is where additional Ray components would be added to the environment 
and references added to them, as needed, in the returned dictionary of configuration data
that will initialize the transform.
For those transforms that don't need this support, the default implementation
simpy returns the CLI parameters used to initialize the runtime instance.

The `computed_execution_stats()` provides an opportunity to augment the statistics
collected and aggregated by the TransformStatistics actor. It is called by the RayOrchestrator
after all files have been processed.




