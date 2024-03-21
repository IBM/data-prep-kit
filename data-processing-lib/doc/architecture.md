# Data Processing Architecture

In this section we cover the high-level architecture, some of the core components.  

Transform implementation and examples are provided in the [tutorial](transform-tutorials.md).

## Architecture

The architecture is a "standard" implementation of [Embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) to
process many input files in parallel using a distribute network of RayWorkers.

![Processing Architecture](processing-architecture.jpg)

The architecture includes the following core components: 

* [RayLauncher](../src/data_processing/ray/transform_launcher.py) accepts and validates 
 CLI parameters to establish the Ray Orchestrator with the proper configuration. 
It uses the following components, all of which can/do define CLI configuration parameters.:
    * [Transform Orchestrator Configuration](../src/data_processing/ray/transform_orchestrator_configuration.py) is responsible 
     for defining and validating infrastructure parameters 
     (e.g., number of workers, memory and cpu, local or remote cluster, etc.). This class has very simple state
     (several dictionaries) and is fully pickleable. As a result framework uses its instance as a
     parameter in remote functions/actors invocation.
    * [DataAccessFactory](../src/data_processing/data_access/data_access_factory.py) - provides the
      configuration for the type of DataAccess to use when reading/writing the input/output data for
      the transforms.  Similar to Transform Orchestrator Configuration, this is a pickleable
      instance that is passed between Launcher, Orchestrator and Workers.
    * [TransformConfiguration](../src/data_processing/ray/transform_runtime.py) - defines specifics
      of the transform implementation including transform implementation class, its short name, any transform-
      specific CLI parameters, and an optional TransformRuntime class, discussed below. 
     
    After all parameters are validated, the ray cluster is started and the DataAccessFactory, TransformOrchestratorConfiguraiton
    and TransformConfiguration are given to the Ray Orchestrator, via Ray remote() method invocation.
    The Launcher waits for the Ray Orchestrator to complete.
* [Ray Orchestrator](../src/data_processing/ray/transform_orchestrator.py) is responsible for overall management of
  the data processing job. It creates the actors, determines the set of input data and distributes the 
  references to the data files to be processed by the workers. More specifically, it performs the following:
  1. Uses the DataAccess instance created by the DataAccessFactory to determine the set of the files 
  to be processed.  
  2. uses the TransformConfiguration to create the TransformRuntime instance 
  3. Uses the TransformRuntime to optionally apply additional configuration (ray object storage, etc) for the configuration
  and operation of the Transform.
  3. uses the TransformOrchestratorConfiguration to determine the set of RayWorkers to create
  to execute transformers in parallel, providing the following to each worker:
      * Ray worker configuration
      * DataAccessFactory 
      * Transform class and its TransformConfiguration containing the CLI parameters and any TransformRuntime additions.
  4. in a load-balanced, round-robin fashion, distributes the names of the input files to the workers for them to transform/process.
   
  Additionally, to provide monitoring of long-running transforms, the orchestrator is instrumented with 
  [custom metrics](https://docs.ray.io/en/latest/ray-observability/user-guides/add-app-metrics.html), that are exported to localhost:8080 (this is the endpoint that 
  Prometheus would be configured to scrape).
  Once all data is processed, the orchestrator will collect execution statistics (from the statistics actor) 
  and build and save it in the form of execution metadata (`metadata.json`). Finally, it will return the execution 
  result to the Launcher.
* [Ray worker](../src/data_processing/ray/transform_table_processor.py) is responsible for 
reading files (as [PyArrow Tables](https://levelup.gitconnected.com/deep-dive-into-pyarrow-understanding-its-features-and-benefits-2cce8b1466c8))
assigned by the orchestrator, applying the transform to the input table and writing out the 
resulting table(s).  Metadata produced by each table transformation is aggregated into
Transform Statistics (below).
* [Transform Statistics](../src/data_processing/ray/transform_statistics.py) is a general 
purpose data collector actor aggregating the numeric metadata from different places of 
the framework (especially metadata produced by the transform).
These statistics are reported as metadata (`metadata.json`) by the orchestrator upon completion.

## Core Components
Some of the core components used by the architecture are definfed here:

* [CLIProvider](../src/data_processing/utils/cli_utils.py) - provides a general purpose
  mechanism for defining, validating and sharing CLI parameters. 
  It is used by the DataAccessFactor and Transform Configuration (below).
* Data Access is an abstraction layer for different data access supported by the framework. The main components
  of this layer are:
  * [Data Access](../src/data_processing/data_access/data_access.py) is the basic interface for the data access, and enables the identification of 
  input files to process, associated output files, checkpointing and general file reading/writing.
    Currently, the framework implements several concrete implementations of the Data Access, including
    [local data support](../src/data_processing/data_access/data_access_local.py) and
    [s3](../src/data_processing/data_access/data_access_s3.py). Additional Data Access implementations can be added as required.
  * [Data Access Factory](../src/data_processing/data_access/data_access_factory.py) is an implementation of the 
    [factory design pattern](https://www.pentalog.com/blog/design-patterns/factory-method-design-pattern/) for creation
    of the data access instances. Data Access Factory, as a CLIProvider,  enables the definition of CLI 
    parameters that configure the instance of Data Access to be created. Data Access factory has very simple state 
    (several dictionaries) and is fully pickleable. The framework uses Data Access Factory instance as a 
    parameter in remote functions/actors invocations.

 
## Transforms
A brief discussion of the Transform components are provided here.
For a more complete discussion, see the [tutorials](transform-tutorials.md).

* [Transform](../src/data_processing/transform/table_transform.py) - defines the methods required
of any transform implementation - `transform()` and `flush()` - and provides the bulk of any transform implementation
convert one Table to 0 or more new Tables.   In general, this is not tied to the above Ray infrastructure 
and so can usually be used independent of Ray. 
* [TransformRuntime ](../src/data_processing/ray/transform_runtime.py) - this class only needs to be
extended/implemented when additional Ray components (actors, shared memory objects, etc.) are used
by the transform. The main method `get_transform_config()` is used to enable these extensions.
* [TransformConfiguration](../src/data_processing/ray/transform_runtime.py) - this is the bootstrap
  class provided to the Launcher that enables the instantiation of the Transform and the TransformRuntime within
  the architecture.  It is a CLIProvider, which allows it to define transform-specific CLI configuration
  that is made available to the Transform's initializer.
 


