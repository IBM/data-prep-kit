# Simplest Transform Tutorial
In this example, we implement a [noop](../../transforms/universal/noop) transform that takes no action
on the input table and returns it unmodified - a _no operation_ (noop).
This effectively enables a copy of a directory tree of
parquet files to an output directory.
This is functionally not too powerful, but allows us to focus
on the minimum requirements for a simple transform that converts
one table to another.  That said, we will show the following:

* How to write the _noop_ transform to generate the output table.
* How to define transform-specific metadata that can be associated
  with each table transformation and aggregated across all transformations
  in a single run of the transform.
* How to define command line arguments that can be used to configure
  the operation of our _noop_ transform.

We will **not** be showing the following:
* The creation of a custom TransformRuntime that would enable more global
  state and/or coordination among the transforms running in other ray actors.
  This will be covered in an advanced tutorial.

The complete task involves the following:
* NOOPTransform - class that implements the specific transformation
* NOOPTableTransformConfiguration - class that provides configuration for the
  NOOPTransform, specifically the command line arguments used to configure it.
* main() - simple creation and use of the TransformLauncher.

(Currently, the complete code for the noop transform used for this
tutorial can be found in the
[noop transform](../../transforms/universal/noop) directory.

Finally, we show to use the command line to run the transform in a local ray cluster

## NOOPTransform

First, let's define the transform class.  To do this we extend
the base abstract/interface class
[AbstractTableTransform](../src/data_processing_ibm/transform/table_transform.py),
which requires definition of the following:
* an initializer (i.e. `init()`) that accepts a dictionary of configuration
  data.  For this example, the configuration data will only be defined by
  command line arguments (defined below).
* the `transform()` method itself that takes an input table produces an output
  table and any associated metadata for that table transformation.

Other methods such as `flush()` need not be overridden/redefined for this simple example.

We start with the simple definition of the class, its initializer and the imports required
by subsequent code:

```python
import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa 
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform

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

Note that in more complex transforms that might, for example, load a hugging face or other model,
or perform other deep initializations, these can be done in the initializer.

Next we define the `transform()` method itself, which includes the addition of some
almost trivial metadata.

```python
    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        if self.sleep is not None:
            time.sleep(self.sleep)
        # Add some sample metadata.
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata
```
The single input to this method is the in-memory pyarrow table to be transformed.
The return of this function is a list of tables and optional metadata.  In this
case of simple 1:1 table conversion the list will contain a single table, the input.
The metadata is a free-form dictionary of keys with numeric values that will be aggregated
by the framework and reported as aggregated job statistics metadata.
If there is no metadata then simply return an empty dictionary.

## NOOPTransformConfiguration

Next we define the `NOOPTransformConfiguration` class and its initializer that define the following:

* The short name for the transform
* The class implementing the transform - in our case NOOPTransform
* Command line argument support.
* The transform runtime class be used.  We will use the `DefaultTableTransformRuntime`
  which is sufficient for most 1:1 table transforms.  Extensions to this class can be
  used when more complex interactions among transform is required.*

First we define the class and its initializer,

```python
short_name = "NOOP"
cli_prefix = f"{short_name}_"

class NOOPTransformConfiguration(DefaultTableTransformConfiguration):
    
    def __init__(self):
        super().__init__(name=short_name, transform_class=NOOPTransform)
        self.params = {}
```
The initializer extends the DefaultTableTransformConfiguration which provides simple
capture of our configuration data and enables picklability through the network.
It also adds a `params` field that will be used below to hold the transform's
configuration data (used in `NOOPTransform.init()` above).

Next, we provide two methods that define and capture the command line configuration that
is specific to the `NOOPTransform`, in this case the number of seconds to sleep during transformation
and an example command line, `pwd`, option holding sensitive data that we don't want reported
in the job metadata produced by the ray orchestrator.
First we define the method establishes the command line arguments.
This method is given a global argument parser to which the `NOOPTransform` arguments are added.
It is good practice to include a common prefix to all transform-specific options (i.e. pii, lang, etc).
In our case we will use `noop_`.

```python
    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{cli_prefix}noop_sleep_sec",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame",
        )
        # An example of a command line option that we don't want included in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            f"--{cli_prefix}noop_pwd",
            type=str,
            default="nothing",
            help="A dummy password which should be filtered out of the metadata",
        )
```
Next we implement a method that is called after the framework has parsed the CLI args
and which allows us to capture the `NOOPTransform`-specific arguments, optionally validate them
and flag that the `pwd` parameter should not be reported in the metadata.

```python
    def apply_input_params(self, args: Namespace) -> bool:
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        if captured.get(sleep_key) < 0:
          print(f"Parameter noop_sleep_sec should be non-negative. you specified {args.noop_sleep_sec}")
          return False
      
        self.params = self.params | captured
        logger.info(f"noop parameters are : {self.params}")
        # Don't publish this in the metadata produced by the ray orchestrator.
        self.remove_from_metadata.append(pwd_key)
        return True
```

## main()

Next, we show how to launch the framework with the `NOOPTransform` using the
framework's `TransformLauncher` class.

```python
if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
    launcher.launch()
```
The launcher requires only an instance of DefaultTableTransformConfiguration
(our `NOOPTransformConfiguration` class).
A single method `launch()` is then invoked to run the transform in a Ray cluster.

## Running

Assuming the above `main()` is placed in `noop_main.py` we can run the transform on data
in COS as follows:

```shell
python noop_main.py --noop_sleep_msec 2 \
  --run_locally True  \
  --s3_cred "{'access_key': 'KEY', 'secret_key': 'SECRET', 'cos_url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud'}" \
  --s3_config "{'input_folder': 'cos-optimal-llm-pile/test/david/input/', 'output_folder': 'cos-optimal-llm-pile/test/david/output/'}"
```
This is a minimal set of options to run locally.
See the [launcher options](launcher-options.md) for a complete list of
transform-independent command line options.
