# Advanced Transform Tutorial

In this example, we implement an [ededup](../../transforms/universal/ededup) transform that
removes duplicate documents across all files. In this tutorial, we will show the following:

* How to write the `ededup` transform to generate the output table.
* How to define transform-specific metadata that can be associated
  with each table transformation and aggregated across all transformations
  in a single run of the transform.
* How to implement custom `TransformRuntime` to create supporting Ray objects and supplement
  transform-specific metadata with the information about this statistics
* How to define command line arguments that can be used to configure
  the operation of our _noop_ transform.

The complete task involves the following:
* EdedupTransform - class that implements the specific transformation
* EdedupRuntime - class that implements custom TransformRuntime to create supporting Ray objects and enhance job output
  statistics
* EdedupTableTransformConfiguration - class that provides configuration for the
  EdedupTransform and EdedupRuntime, including transform runtime class and the command line arguments used to
  configure them.
* main() - simple creation and use of the TransformLauncher.

(Currently, the complete code for the noop transform used for this
tutorial can be found in the
[ededup transform](../../transforms/universal/ededup) directory.

Finally, we show to use the command line to run the transform in a local ray cluster

## HashFilter

One of the basic components of exact dedup implementation is a cache of hashes. That is why we will start
from implementing this support actor. The implementation is fairly straight forward and can be
found [here](../../transforms/universal/ededup/src/ededup_transform.py)

## EdedupTransform

First, let's define the transform class.  To do this we extend
the base abstract/interface class
[AbstractTableTransform](../src/data_processing/transform/table_transform.py),
which requires definition of the following:
* an initializer (i.e. `init()`) that accepts a dictionary of configuration
  data.  For this example, the configuration data will only be defined by
  command line arguments (defined below).
* the `transform()` method itself that takes an input table and produces an output
  table and any associated metadata for that table transformation.

Other methods such as `flush()` need not be overridden/redefined for this example.

We start with the simple definition of the class, its initializer and the imports required
by subsequent code:

```python
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
import ray
from data_processing.data_access import DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    RayUtils,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import GB, TransformUtils
from ray.actor import ActorHandle


class EdedupTransform(AbstractTableTransform):

    def __init__(self, config: dict):
        super().__init__(config)
        self.doc_column = config.get("doc_column", "")
        self.hashes = config.get("hashes", [])
```
The `EdedupTransform` class extends the `AbstractTableTransform`, which defines the required methods.

For purposes of the tutorial and to simulate a more complex processing
job, our initializer allows our transform to be configurable
with document column name and a list of hash actors during the call to `transform()`.
Configuration is provided by the framework in a dictionary provided to the initializer.
Below we will cover how `doc_column` and `hashes` arguments are made available to the initializer.

Next we define the `transform()` method itself, which includes the addition of some
metadata.

```python
    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        if not TransformUtils.validate_columns(table=table, required=[self.doc_column]):
            return [], {}
        # Inner variables
        hashes = set()
        unique = []
        hd = {}
        # Compute unique hashes for the table
        for text in table[self.doc_column]:
            # Compute doc hash
            h = TransformUtils.str_to_hash(TransformUtils.normalize_string(str(text)))
            if h not in hashes:  # Processing this hash for the first time
                hashes.add(h)  # Remember it locally
                hd[h] = str(text)
                if len(hd) >= REQUEST_LEN:  # time to check remotely
                    unique = unique + self._process_remote_hashes(hd=hd)
                    hd = {}
        if len(hd) > 0:  # Process remaining hashes
            unique = unique + self._process_remote_hashes(hd=hd)
    
        # Remove duplicates
        unique_set = set(unique)
        mask = [False] * table.num_rows
        index = 0
        for text in table[self.doc_column]:
            str_text = str(text)
            if str_text in unique_set:
                mask[index] = True
                unique_set.remove(str_text)
            index += 1
        # Create output table
        out_table = table.filter(mask)
        # report statistics
        stats = {"source_documents": table.num_rows, "result_documents": out_table.num_rows}
        return [out_table], stats
```
The single input to this method is the in-memory pyarrow table to be transformed.
The return of this function is a list of tables and optional metadata.  In this
case of simple 1:1 table conversion the list will contain a single table, result of removing
duplicates from input table.

The metadata is a free-form dictionary of keys with numeric values that will be aggregated
by the framework and reported as aggregated job statistics metadata.
If there is no metadata then simply return an empty dictionary.

## EdedupRuntime

First, let's define the transform runtime class.  To do this we extend
the base abstract/interface class
[DefaultTableTransformRuntime](../src/data_processing/ray/transform_runtime.py),
which requires definition of the following:
* an initializer (i.e. `init()`) that accepts a dictionary of configuration
  data.  For this example, the configuration data will only be defined by
  command line arguments (defined below).
* the `get_transform_config()` method that takes `data_access_factory`, `statistics actor`, and
  `list of files to process` and produces a dictionary of parameters used by transform.
* the `compute_execution_stats()` method that takes take a dictionary of metadata, enhances it and
  produces an enhanced metadata dictionary.

We start with the simple definition of the class and its initializer

```python
class EdedupRuntime(DefaultTableTransformRuntime):

    def __init__(self, params: dict[str, Any]):
        super().__init__(params)
        self.filters = []
```
Next we define the `get_transform_config()` method, which, in this case, creates supporting Ray Actors and
adds their handles to the transform parameters

```python
    def get_transform_config(
        self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        self.filters = RayUtils.create_actors(
            clazz=HashFilter,
            params={},
            actor_options={"num_cpus": self.params.get("hash_cpu", 0.5)},
            n_actors=self.params.get("num_hashes", 1),
        )
        return {"hashes": self.filters} | self.params
```
Inputs to this method includes a set of parameters, that moght not be needed for this transformer, but
rather a superset of all parameters that can be used by different implementations of transform runtime (
see for example [fuzzy dedup](../../transforms/universal/fdedup), etc).
The return of this function is a dictionary information for transformer initialization. In this
implementation we add additional parameters to the input dictionary, but in general, it can be a completely
new dictionary build here

Finally we define the `compute_execution_stats()` method, which which enhances metadata collected by statistics
class

```python
    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
    # Get filters stats
    sum_hash = 0
    sum_hash_mem = 0
    remote_replies = [f.get_hash_size.remote() for f in self.filters]
    while remote_replies:
        # Wait for replies
        ready, not_ready = ray.wait(remote_replies)
        for r in ready:
            h_size, h_memory = ray.get(r)
            sum_hash = sum_hash + h_size
            sum_hash_mem = sum_hash_mem + h_memory
        remote_replies = not_ready
    dedup_prst = 100 * (1.0 - stats.get("result_documents", 1) / stats.get("source_documents", 1))
    return {"number of hashes": sum_hash, "hash memory, GB": sum_hash_mem, "de duplication %": dedup_prst} | stats
```
Input to this method is a dictionary of metadata collected by statistics object. It then enhances it by information
collected by hash actors and custom computations based on statistics data.

## EdedupTableTransformConfiguration

The final class we need to implement is `EdedupTableTransformConfiguration` class and its initializer that
define the following:

* The short name for the transform
* The class implementing the transform - in our case EdedupTransform
* The transform runtime class be used - in our case EdedupRuntime
* Command line argument support.

First we define the class and its initializer,

```python
short_name = "ededup"
cli_prefix = f"{short_name}_"

class EdedupTableTransformConfiguration(DefaultTableTransformConfiguration):
    def __init__(self):
        super().__init__(name=short_name, runtime_class=EdedupRuntime, transform_class=EdedupTransform)
        self.params = {}
```

The initializer extends the DefaultTableTransformConfiguration which provides simple
capture of our configuration data and enables picklability through the network.
It also adds a `params` field that will be used below to hold the transform's
configuration data (used in `EdedupRuntime.init()` above).

Next, we provide two methods that define and capture the command line configuration that
is specific to the `EdedupTransform`, in this case the number of seconds to sleep during transformation.
First we define the method establishes the command line arguments.
This method is given a global argument parser to which the `EdedupTransform` arguments are added.
It is good practice to include a common prefix to all transform-specific options (i.e. pii, lang, etc).
In our case we will use `noop_`.

```python
    def add_input_params(self, parser: ArgumentParser) -> None:
      parser.add_argument(f"--{cli_prefix}hash_cpu", type=float, default=0.5, help="number of CPUs per hash")
      parser.add_argument(f"--{cli_prefix}num_hashes", type=int, default=0, help="number of hash actors to use")
      parser.add_argument(f"--{cli_prefix}doc_column", type=str, default="contents", help="key for accessing data")
```
Next we implement a method that is called after the framework has parsed the CLI args
and which allows us to capture the `EdedupTransform`-specific arguments and optionally validate them.

```python
    def apply_input_params(self, args: Namespace) -> bool:
      captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
      self.params = self.params | captured
      if self.params["num_hashes"] <= 0:
        logger.info(f"Number of hashes should be greater then zero, provided {args.num_hashes}")
        return False
      logger.info(f"exact dedup params are {self.params}")
      return True
```

## main()

Next, we show how to launch the framework with the `EdedupTransform` using the
framework's `TransformLauncher` class.

```python
if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=EdedupTransformConfiguration())
    launcher.launch()
```
The launcher requires only an instance of DefaultTableTransformConfiguration
(our `EdedupTransformConfiguration` class).
A single method `launch()` is then invoked to run the transform in a Ray cluster.

## Running

Assuming the above `main()` is placed in `ededup_transform.py` we can run the transform on data
in COS as follows:

```shell
python ededup_transform.py --hash_cpu 0.5 --num_hashes 2 --doc_column "contents" \
  --run_locally True  \
  --s3_cred "{'access_key': 'KEY', 'secret_key': 'SECRET', 'cos_url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud'}" \
  --s3_config "{'input_folder': 'cos-optimal-llm-pile/test/david/input/', 'output_folder': 'cos-optimal-llm-pile/test/david/output/'}"
```
This is a minimal set of options to run locally.
See the [launcher options](launcher-options.md) for a complete list of
transform-independent command line options.
