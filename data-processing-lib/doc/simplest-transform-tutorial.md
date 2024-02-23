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

The complete task involves create the following:
* NOOPTransform - class that implements the specific transformation
* NOOPTableTransformConfiguration - class that provides configuration for the 
NOOPTransform, specifically the command line arguments used to configure it.
* main() - simple creation and use of the RayLauncher. 

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

We start with the simple definition of the class and the imports required
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
...
```
The `NOOPTransform` class extends the `AbstractTableTransform`, which defines
the require methods.

For purposes of the tutorial and to simulate a more complex processing
job, we will allow our transform to be configurable
with an amount of seconds to sleep/delay during the call to `transform()`.
As such we have the following initializer,
```python
    def __init__(self, config: dict[str, Any]):
        self.sleep = config.get("sleep", 1)
```
Below we will cover how this `sleep` argument is made available to the initializer.
Next we define the `transform()` method itself, which includes addition of some
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
Next we define the `NOOPTransformConfiguration`  class the defines the following:

* The class implementing the transform - in our case NOOPTransform
* Command line argument support.
* The transform runtime class be used.  We will use the `DefaultTableTransformRuntime`
which is sufficient for most 1:1 table transforms.  Extensions to this class can be
used when more complex interactions among transform is required.*
* 
```python
...
class NOOPTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(runtime_class=DefaultTableTransformRuntime, transform_class=NOOPTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the NOOPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--noop_sleep_sec",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.noop_sleep_sec <= 0:
            print(f"Parameter noop_sleep_sec should be greater then 0, you specified {args.noop_sleep_sec}")
            return False
        self.params["sleep"] = args.noop_sleep_sec
        print(f"noop parameters are : {self.params}")
        return True
```