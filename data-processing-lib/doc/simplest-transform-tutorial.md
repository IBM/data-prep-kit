# Simplest Transform Tutorial
In this example, we implement a _noop_ transform that takes no action
on the input table and returns its unmodified.
This effectively enables a copy of a directory tree of 
parquet files to an output directory.
This is functionally not too interesting but allows use to focus
on the minimum requirements for a simple transform that converts
one table to another.  That said, we will show the following:

* How to define command line arguments that can be used to configure
the operation of our _noop_ transform.
* How to define transform-specific metadata that can be associated
with each table transformation and aggregated across all transformations
in a single run of the transform.

We will **not** be showing the following:
* The creation of a custom TransformRuntime that would enable more global
state and/or coordination among the transforms running in other RayActors.
This will be covered in an advanced tutorial.

The complete task involves create the following:
* NOOPTransform - class that implements the specific transformation
* NOOPTableTransformConfiguration - class that provides configuration for the 
NOOPTransform, specifically the command line arguments used to configure it.
* main() - simple creation and use of the TransformLauncher. 

### NOOPTransform

First, let's define the transform class.  To do this we extend
the base abstract/interface class 
[AbstractTableTransform](../src/data_processing/transform/table_transform.py),
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
job, we allow our transform to be configurable
with an amount of seconds to sleep/delay during the call to `transform()`.
Configuration is provided by the framework in a dictionary provided to the initializer.
Below we will cover how this `sleep` argument is made available to the initializer.

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
The return of this function is a list of tables and optional metadata.  In this
case of simple 1:1 table conversion the list will contain a single table, the input.
The metadata is a free-form dictionary of keys with numeric values that will be aggregated
by the framework and reported as aggregated job statistics metadata. 
If there is not metadata then simply return an empty dictionary.

### NOOPTransformConfiguration
Next we define the `NOOPTransformConfiguration` class and its initializer that define the following:

* The short name for the transform
* The class implementing the transform - in our case NOOPTransform
* Command line argument support.
* The transform runtime class be used.  We will use the `DefaultTableTransformRuntime`
which is sufficient for most 1:1 table transforms.  Extensions to this class can be
used when more complex interactions among transform is required.*

First we define the class and its initializer,
```python
...
class NOOPTransformConfiguration(DefaultTableTransformConfiguration):
    
    def __init__(self):
        super().__init__(name="NOOP", 
                         runtime_class=DefaultTableTransformRuntime, 
                         transform_class=NOOPTransform)
        self.params = {}

```
The initializer extends the DefaultTableTransformConfiguration which provides simple
capture of our configuration data and enables picklability through the network.

Next, we provide two methods that define and capture the command line configuration that 
is specific to the `NOOPTransform`, in this case the number of seconds to sleep during transformation.
First we define the method establishes the command line arguments.  
This method is given a global argument parser to which the `NOOPTransform` arguments are added.
It is good practice to include common prefix to all transform-specific options (i.e. pii, lang, etc).
In our case we will use `noop_`.
```python
    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--noop_sleep_sec",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )
```
Next we implement a method that is called after the framework has parsed the CLI args
and which allows us to capture the `NOOPTransform`-specific arguments and optionally validate them.
```python
    def apply_input_params(self, args: Namespace) -> bool:
        if args.noop_sleep_sec <= 0:
            print(f"Parameter noop_sleep_sec should be greater then 0, you specified {args.noop_sleep_sec}")
            return False
        self.params["sleep"] = args.noop_sleep_sec
        print(f"noop parameters are : {self.params}")
        return True
```
That's it, the `NOOPTransformConfiguration` is complete.

### main()
Lastly we show how to launch the framework with the `NOOPTransform` using the 
framework's `TransformLauncher` class.
```python
...
if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
    launcher.launch()
```
The launcher requires only an instance of DefaultTableTransformCOnfiguration (our `n` class).  
A single method `launch()` is then invoked to run the transform in a Ray cluster.

### Running
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
