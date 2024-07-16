# Simplest Transform Tutorial
In this example, we implement a [noop](../../transforms/universal/noop) 
transform that takes no action
on the input datum and returns it unmodified - a _no operation_ (noop).
This effectively enables a copy of a directory tree of
files to an output directory.
This is functionally not too powerful, but allows us to focus
on the minimum requirements for a transform. 

**NOTE**: What follows is a discussion of pyarrow Table transform that
will run in either the Ray or Python [runtimes](transform-runtimes.md).
Mapping the tutorial to byte arrays would use the 
[AbstractBinaryTransform](../python/src/data_processing/transform/binary_transform.py)
instead of `AbstractTableTransform` (a sub-class of the former).
Mapping the tutorial to a Spark runtime would use 
[AbstractSparkTransform](../spark/src/data_processing_spark/runtime/spark/spark_transform.py)
instead of `AbstractTableTransform` and use `DataFrame` instead of pyarrow Table as
the `DATA` type.  In addition, the 
[SparkTransformLauncher](../spark/src/data_processing_spark/runtime/spark/spark_launcher.py)
would be used in place of the `RayTransformLauncher` and `PythonTransformLauncher` shown below.

That said, we will show the following:

* How to write the _noop_ transform to generate the output table.
* How to define transform-specific metadata that can be associated
  with each table transformation and aggregated across all transformations
  in a single run of the transform.
* How to define command line arguments that can be used to configure
  the operation of our _noop_ transform.

We will **not** be showing the following:
* The creation of a custom `TransformRuntime` that would enable more global
  state and/or coordination among the transforms running in other Ray actors.
  This will be covered in an advanced tutorial.

The complete task involves the following:
* `noop_main.py` - a empty file to start writing code as described below 
* `NOOPTransform` - class that implements the specific transformation
* `NOOPTableTransformConfiguration` - class that provides configuration for the
  `NOOPTransform`, specifically the command line arguments used to configure it.
* `main()` - simple creation and use of the `TransformLauncher`.

(Currently, the complete code for the noop transform used for this
tutorial can be found in the
[noop transform](../../transforms/universal/noop) directory.

Finally, we show how to use the command line to run the transform in a local ray cluster.

> **Note:** You will need to run the setup commands in the [`README`](../ray/README.md) before running the following examples.


## `NOOPTransform`

First, let's define the transform class.  To do this we extend
the base abstract/interface class
[`AbstractTableTransform`](../python/src/data_processing/transform/table_transform.py),
which requires definition of the following:

* an initializer (i.e. `init()`) that accepts a dictionary of configuration
  data.  For this example, the configuration data will only be defined by
  command line arguments (defined below).
* the `transform()` method itself that takes an input table and produces an output
  table with any associated metadata for that table transformation.

Other methods such as `flush()` need not be overridden/redefined for this simple example.

We start with the simple definition of the class, its initializer and the imports required
by subsequent code:

```python
import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing_ray.runtime.ray.runtime_configuration import (
  RayTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger


class NOOPTransform(AbstractTableTransform):

  def __init__(self, config: dict[str, Any]):
    self.sleep = config.get("sleep", 1)
```
The `NOOPTransform` class extends the `AbstractTableTransform`, which defines the required methods.

For purposes of the tutorial and to simulate a more complex processing
job, our initializer allows our transform to be configurable
with an amount of seconds to sleep/delay during the call to `transform()`.
Configuration is provided by the framework in a dictionary provided to the initializer.
Below we will cover how this `sleep` argument is made available to the initializer.

Note that in more complex transforms that might, for example, load a Hugging Face or other model,
or perform other deep initializations, these can be done in the initializer.

Next we define the `transform()` method itself, which includes the addition of some
almost trivial metadata.

```python
    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        if self.sleep is not None:
            time.sleep(self.sleep)
        # Add some sample metadata.
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata
```
The single input to this method is the in-memory pyarrow table to be transformed.
The return value of this method is a list of tables and optional metadata.  In this
case, we are doing a simple 1:1 table conversion, so the list will contain a single table, the input table.
The metadata is a free-form dictionary of keys with numeric values that will be aggregated
by the framework and reported as aggregated job statistics metadata.
If there is no metadata then simply return an empty dictionary.

## `NOOPTransformConfiguration`

Next we define the `NOOPTransformConfiguration` class and its initializer that defines the following:

* The short name for the transform
* The class implementing the transform - in our case `NOOPTransform`
* Command line argument support.
 
We also define the `NOOPRayTransformationConfiguration` so we can run the transform
in the Ray runtime as well.  This adds allows the option to add a transform-specific
Ray runtime class allowing more complex distributed memory and data sharing operations.
The NOOP transform will not make use of this so is a simple extension.:

First we define the pure python transform configuration  class and its initializer,

```python
short_name = "noop"
cli_prefix = f"{short_name}_"
sleep_key = "sleep_sec"
pwd_key = "pwd"
sleep_cli_param = f"{cli_prefix}{sleep_key}"
pwd_cli_param = f"{cli_prefix}{pwd_key}"


class NOOPTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=NOOPTransform,
            remove_from_metadata=[pwd_key],
        )
```

The initializer extends the `TransformConfiguration` that provides simple
capture of our configuration data and enables the ability to pickle through the network.
It also adds a `params` field that will be used below to hold the transform's
configuration data (used in `NOOPTransform.init()` above).

Next, we provide two methods that define and capture the command line configuration that
is specific to the `NOOPTransform`, in this case the parameters are the number of seconds to sleep during transformation
and an example command line parameter, `pwd` ("password"), option holding sensitive data that we don't want reported
in the job metadata produced by the Ray orchestrator.

The first method establishes the command line arguments.
It is given a global argument parser to which the `NOOPTransform` arguments are added.
It is a good practice to include a common prefix to all transform-specific options (i.e. pii, lang, etc).
In our case we will use `noop_`.

```python
    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{sleep_cli_param}",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
        parser.add_argument(
            f"--{pwd_cli_param}",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )
```
Next we implement a method that is called after the CLI args are parsed (usually by one
of the runtimes) and which allows us to capture the `NOOPTransform`-specific arguments. 
 

```python

    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
            print(f"Parameter noop_sleep_sec should be non-negative. you specified {args.noop_sleep_sec}")
            return False
        self.params = captured
        return True
```
## Runtime Launching
To run the transform on a set of input data, we use one of the runtimes, each described below.

### Python Runtime
To run in the python runtime, we need to create the instance of `PythonTransformLauncher`
using the `NOOPTransformConfiguration`, and launch it as follows:

```python
from data_processing.runtime.pure_python import PythonTransformLauncher
if __name__ == "__main__":
    launcher = PythonTransformLauncher(runtime_config=NOOPTransformConfiguration())
    launcher.launch()
```

Assuming the above `main` code is placed in `noop_main.py` we can run the transform on some test data. We'll use data in the repo for the noop transform
and create a temporary directory to hold the output:
```shell
export DPK_REPOROOT=...
export NOOP_INPUT=$DPK_REPOROOT/transforms/universal/noop/python/test-data/input
```
To run
```shell
python noop_main.py --noop_sleep_sec 2 \
  --data_local_config "{'input_folder': '"$NOOP_INPUT"', 'output_folder': '/tmp/noop-output'}"
```
See the [python launcher options](python-launcher-options.md) for a complete list of
transform-independent command line options.

### Ray Runtime
To run in the Ray runtime, instead of creating the `PythonTransformLauncher`
we use the `RayTransformLauncher`.
as follows:
```python
class NOOPRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=NOOPTransformConfiguration())

from data_processing_ray.runtime.ray import RayTransformLauncher
if __name__ == "__main__":
    launcher = RayTransformLauncher(runtime_config=NOOPRayTransformConfiguration())
    launcher.launch()
```
We can run this with the same command as for the python runtime but to run in local Ray
add the `--run_locally True` option.
```shell
python noop_main.py --noop_sleep_sec 2 \
  --data_local_config "{'input_folder': '"$NOOP_INPUT"', 'output_folder': '/tmp/noop-output'}" --run_locally True
```
which will start local ray instance ( ray should be pre [installed](https://docs.ray.io/en/latest/ray-overview/installation.html)).
See the [ray launcher options](ray-launcher-options.md) for a complete list of
transform-independent command line options.
