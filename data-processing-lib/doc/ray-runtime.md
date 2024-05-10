# Ray Runtime 
The Ray runtime includes the following set of components:

* [RayTransformLauncher](../src/data_processing/runtime/ray/transform_launcher.py) - this is a 
class generally used to implement `main()` that makes use of a `TransformConfiguration` to 
start the Ray runtime and execute the transform over the specified set of input files.
The RayTransformLauncher is created using a `RayTransformConfiguration` instance.
* [RayTransformConfiguration](../src/data_processing/runtime/ray/transform_configuration.py) - this 
class extends transform's base TransformConfiguration implementation to add an optional 
`TranformRuntime` (see next) class to be used by the transform implementation.
* [TransformRuntime](../src/data_processing/runtime/ray/transform_runtime.py) - 
this provides the ability for the transform implementor to create additional Ray resources 
and include them in the configuration used to create a transform
(see, for example, [exact dedup](../../transforms/universal/ededup/src/ededup_transform.py)).
This also provide the ability to supplement the statics collected by
[Statistics](../src/data_processing/runtime/ray/transform_statistics.py) (see below).

Roughly speaking the following steps are completed to establish transforms in the RayWorkers

1. Launcher parses the CLI parameters using an ArgumentParser configured with its own CLI parameters 
along with those of the Transform Configuration, 
2. Launcher passes the Transform Configuration and CLI parameters to the [RayOrchestrator](../src/data_processing/runtime/ray/transform_orchestrator.py)
3. RayOrchestrator creates the Transform Runtime using the Transform Configuration and its CLI parameter values
4. Transform Runtime creates transform initialization/configuration including the CLI parameters,  
and any Ray components need by the transform.
5. [RayWorker](../src/data_processing/runtime/ray/transform_table_processor.py) is started with configuration from the Transform Runtime.
6. RayWorker creates the Transform using the configuration provided by the Transform Runtime.
7. Statistics is used to collect the statistics submitted by the individual transform, that 
is used for building execution metadata.

![Processing Architecture](processing-architecture.jpg)

## Ray Transform Launcher
The [RayTransformLauncher](../src/data_processing/runtime/ray/transform_launcher.py) uses the Transform Configuration
and provides a single method, `launch()`, that kicks off the Ray environment and transform execution coordinated 
by [orchestrator](../src/data_processing/runtime/ray/transform_orchestrator.py).
For example,
```python
launcher = RayTransformLauncher(YourTransformConfiguration())
launcher.launch()
```
Note that the launcher defines some additional CLI parameters that are used to control the operation of the 
[orchestrator and workers](../src/data_processing/runtime/ray/transform_orchestrator_configuration.py) and 
[data access](../src/data_processing/data_access/data_access_factory.py).  Things such as data access configuration,
number of workers, worker resources, etc.
Discussion of these options is beyond the scope of this document 
(see [Launcher Options](ray-launcher-options) for a list of available options.)

## Ray Transform Configuration
In general, a transform should be able to run in both the python and Ray runtimes.
As such we first define the python-only transform configuration, which will then
be used by the Ray-runtime-specific transform configuration. 
The python transform configuration implements  
[TransformConfiguration](../src/data_processing/transform/transform_configuration.py)
and deifnes with transform-specific name, and implementation 
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
Next we define the Ray-runtime specific transform configuration as an exension of
the RayTransformConfiguration and uses the `YourTransformConfiguration` above.
```python
    
class YourTransformConfiguration(RayTransformConfiguration):
    def __init__(self):
        super().__init__(YourTransformConfiguration(),
                         runtime_class=YourTransformRuntime
```
This class provides the ability to create the instance of `YourTransformRuntime` class (see below)
as needed by the Ray runtime.  Note, that not all transforms will require a `runtime_class`
and can omit this parameter to default to an acceptable runtime class.
Details are covered in the [advanced transform tutorial](advanced-transform-tutorial.md).

## Transform Runtime
The 
[DefaultTableTransformRuntime](../src/data_processing/runtime/ray/transform_runtime.py)
class is provided and will be 
sufficient for many use cases, especially 1:1 table transformation.
However, some transforms will require use of the Ray environment, for example,
to create additional workers, establish a shared memory object, etc.
Of course, these transforms will generally not run outside of Ray environment. 

```python
class DefaultTableTransformRuntime:

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

## Exceptions
A transform may find that it needs to signal error conditions.
For example, if a referenced model could not be loaded or
a given table does not have the expected column.
In general, it should identify such conditions by raising an exception. 
With this in mind, there are two types of exceptions:

1. Those that would not allow any tables to be processed (e.g. model loading problem).
2. Those that would not allow a specific table to be processed (e.g. missing column).

In the first situation the transform should throw an exception from the initializer, which
will cause the Ray framework to terminate processing of all tables. 
In the second situation (identified in the `transform()` or `flush()` methods), the transform
should throw an exception from the associated method.  This will cause only the
error-causing
table to be ignored and not written out, but allow continued processing of tables by 
the transform.
In both cases, the framework will log the exception as an error.


