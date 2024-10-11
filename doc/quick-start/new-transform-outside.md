# Creating a New Transform   

Here we show how to create a new transform outside of this repository
using the Data Prep Kit libraries published on [pypi.org](pypi.org). 

Before proceeding, you should review the 
[architectural overview](../../data-processing-lib/doc/overview.md)
for the details on 
[transforms](../../data-processing-lib/doc/transforms.md)
and 
[runtimes](../../data-processing-lib/doc/transform-runtimes.md).
In addition, the 
[simple transform tutorial](../../data-processing-lib/doc/simplest-transform-tutorial.md)
will be useful.

Depending on the target runtime for the transform, you will need one of the following
dependencies:

* `data-prep-toolkit` - base transform and python runtime framework.
* `data-prep-toolkit-ray` - ray extensions for ray runtime. Depends on `data-prep-toolkit`.
* `data-prep-toolkit-spark` - spark extensions for spark runtime. Depends on `data-prep-toolkit`.

Note the use of `-toolkit` instead of `-kit` due to name collisions on pypi.org.

For this quickstart, we will implement a simple transform that adds a column 
containing a string provided on the command line.

### Create a virtual environment

To start, create a `requirements.txt` file containing the following
```
data-prep-toolkit==0.2.0
```
then
```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Create python transform
We create the following classes
1. `HelloTransform` - implements the table transformation
2. `HelloTransformConfiguration` - defines CLI arguments to configure HelloTransform

```python

from typing import Any
from data_processing.transform import AbstractTableTransform
import pyarrow as pa
from data_processing.utils import TransformUtils

class HelloTransform(AbstractTableTransform):
    '''
    Extends the base table transform class to implement the configurable transform() method.
    '''
    def __init__(self, config:dict):
        self.who = config.get("who", "World")
        self.column_name = config.get("column_name", "greeting")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        # Create a new column with each row holding the who value
        new_column = ["Hello " + self.who + "!"] * table.num_rows
        # Append the column to create a new table with the configured name.
        table = TransformUtils.add_column(table=table, name=self.column_name, content=new_column)
        return [table], {}

import argparse
from data_processing.transform import  TransformConfiguration

class HelloTransformConfiguration(TransformConfiguration):
    '''
    Extends the configuration class to add command line configuration parameters for this transfomr
    '''
    def __init__(self):
        super().__init__(
            name="hello",
            transform_class=HelloTransform,
        )

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        # Define the command line arguments used by this transform
        # We use the ac_ CLI prefix to help distinguish from other runtime CLI parameters
        parser.add_argument(
            "--ac_who",
            type=str, required=False, default="World",
            help="Who to say hello to."
        )

        parser.add_argument(
            "--ac_column_name",
            type=str, required=False, default="greeting", help="Name of column to add"
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        dargs = vars(args)
        # Select this transform's command line arguments from all those
        # provided on the command line.
        self.params = {
            "who": dargs.get("ac_who",None),
            "column_name": dargs.get("ac_column_name",None)
        }
        return True


```

### Python Runtime
To run the transform in the pure python runtime, we create 
1. `HelloPythonTransformConfiguration` class - defines configuration for the python runtime
2. `main` - launches the python runtime with our transform.

```python
from data_processing.runtime.pure_python import PythonTransformRuntimeConfiguration, PythonTransformLauncher
from hello_transform import HelloTransformConfiguration

class HelloPythonConfiguration(PythonTransformRuntimeConfiguration):
    '''
    Configures the python runtime to use the Hello transform
    '''
    def __init__(self):
        super().__init__(transform_config=HelloTransformConfiguration())


if __name__ == "__main__":
    # Create the runtime launcher to use the HelloTransform
    launcher = PythonTransformLauncher(HelloPythonConfiguration())
    launcher.launch()
```

### Running 
In the following, `parquet-tools` will be helpful.  Install with 
```shell
% source venv/bin/activate
(venv) % pip install parquet-tools
```
We will the transform a single parquet file in a directory named 
`input`.
The directory may contain more than one parquet file, 
in which case they will all be processed.
We can examine the input as follows:
```shell
% source venv/bin/activate
(venv) % ls input
test1.parquet
(venv) % parquet-tools show input/test1.parquet
+-----------------------------------+
| title                             |
|-----------------------------------|
| https://poker                     |
| https://poker.fr                  |
| https://poker.foo.bar             |
| https://abc.efg.com               |
| http://asdf.qwer.com/welcome.htm  |
| http://aasdf.qwer.com/welcome.htm |
| https://zxcv.xxx/index.asp        |
+-----------------------------------+
```
Next we run the transform to 
1. read file(s) from the `input` directory 
2. write transformed file(s) to the `output` directory
3. add a column named `hello` containing `Hello David!`, using the `--ac_column_name` and `--ac_who` CLI parameters. 
```shell
(venv) % mkdir output
(venv) % python hello_transform.py  --data_local_config \
    "{ 'input_folder': 'input', 'output_folder': 'output'}" \
    --ac_who David \
    --ac_column_name hello
15:06:59 INFO - pipeline id pipeline_id
15:06:59 INFO - job details {'job category': 'preprocessing', 'job name': 'add_column', 'job type': 'pure python', 'job id': 'job_id'}
15:06:59 INFO - code location None
15:06:59 INFO - data factory data_ is using local data access: input_folder - input output_folder - output
15:06:59 INFO - data factory data_ max_files -1, n_sample -1
15:06:59 INFO - data factory data_ Not using data sets, checkpointing False, max files -1, random samples -1, files to use ['.parquet'], files to checkpoint ['.parquet']
15:06:59 INFO - orchestrator add_column started at 2024-07-10 15:06:59
15:06:59 INFO - Number of files is 1, source profile {'max_file_size': 0.0007181167602539062, 'min_file_size': 0.0007181167602539062, 'total_file_size': 0.0007181167602539062}
15:07:06 INFO - Completed 1 files (100.0%) in 0.12749731938044231 min
15:07:06 INFO - done flushing in 7.867813110351562e-06 sec
15:07:06 INFO - Completed execution in 0.1275073528289795 min, execution result 0
(venv) % ls output
metadata.json	test1.parquet
(venv) % parquet-tools show output/test1.parquet 
+-----------------------------------+--------------+
| title                             | hello        |
|-----------------------------------+--------------|
| https://poker                     | Hello David! |
| https://poker.fr                  | Hello David! |
| https://poker.foo.bar             | Hello David! |
| https://abc.efg.com               | Hello David! |
| http://asdf.qwer.com/welcome.htm  | Hello David! |
| http://aasdf.qwer.com/welcome.htm | Hello David! |
| https://zxcv.xxx/index.asp        | Hello David! |
+-----------------------------------+--------------+
```

