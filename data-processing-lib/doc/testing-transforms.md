# Testing Transforms
A test framework is provided as part of the library in the `data_processing_test` package.
Transform testing makes use of an `AbstractTransformTest` super-class that defines 
the generic tests that should be common to all transforms.
Initially this means testing the transformation of one or more in-memory 
input tables to a one or more output tables and one or more metadata dictionaries.

The `AbstractTransformTest` class defines the `test_*(...)` methods and makes use 
of pytest fixtures to define test cases (i.e. inputs) applied to the test method(s).
Each `test_*(...)` method (currently only one) has an associated abstract
`get_test_*_fixtures()` method that must be implemented by the specific
transform implementation test to define the various sets of inputs tested.
This approach allows the definition of new generic transform tests that existing
transform implementation tests will easily leverage.

The first (currently only test) is a the `test_transform()` method that takes the
following inputs:
* the transform implementation being tested, properly configured with the configuration
dictionary for the associated test data.
* a list of N (1 or more) input tables to be processed with the transform's `transform(Table)` method.
* The expected list of accumulated tables across the N calls to 
`transform(Table)` and the single finalizing call to the transform's `flush()` method.
In the case where the `transform()` returns an empty list, no associated expected Table 
should be included in this list. 
* The expected list of accumulated metadata dictionaries across the N calls to `transform(Table)`
  and the single finalizing call to the transform's `flush()` method.  This list should be of
length N+1 for the N calls to `transform(Table)` plus the finalizing call to `flush()`.

As an example, consider the `NOOPTransformTest` developed as ane example of the testing
framework.

```python
from typing import Tuple

import pyarrow as pa
from data_processing.test_support import AbstractTransformTest
from noop_transform import NOOPTransform

# Define the test input and expected outputs
table = pa.Table.from_pydict({"name": pa.array(["Tom"]), "age": pa.array([23])})
expected_table = table  # We're a noop after all.
expected_metadata_list = [{"nfiles": 1, "nrows": 1}, {}]  # transform() result  # flush() result


class TestNOOPTransform(AbstractTransformTest):

  # Define the method that provides the test fixtures to the test from the super class.
  def get_test_transform_fixtures(self) -> list[Tuple]:
    fixtures = [
      (NOOPTransform({"sleep": 0}), [table], [expected_table], expected_metadata_list),
      (NOOPTransform({"sleep": 1}), [table], [expected_table], expected_metadata_list),
    ]
    return fixtures
```
In the above we use the `NOOPTransform` to process the single input `table`, to produce
the expected table `expected_table` and list of metadata in `expected_metadata_list`, 
The NOOPTransform has no configuration that effects the transformation of input to
output. However, in general this will not be the case and a transform may have different
configurations and associated test data.  For example, a transform might be configurable
to use different models and perhaps as a result have different results. 

Once the test class is defined you may run the test from your IDE or from the command line... 
```shell
% cd .../data-prep-lab/transforms/universal/noop/src
% make venv
% source venv/bin/activate
(venv)% export PYTHONPATH=.../data-prep-lab/transforms/universal/noop/src
(venv)% pytest test/test_noop.py 
================================================================================ test session starts ================================================================================
platform darwin -- Python 3.10.11, pytest-8.0.2, pluggy-1.4.0
rootdir: /Users/dawood/git/data-prep-lab/transforms/universal/noop
plugins: cov-4.1.0
collected 2 items                                                                                                                                                                   

test/test_noop.py ..                                                                                                                                                          [100%]

================================================================================= 2 passed in 0.83s =================================================================================
(venv) % 
```
Finally, the tests should be runnable with the `Makefile`  as follows:
```shell
$ make test
source venv/bin/activate;			\
	export PYTHONPATH=../src:.:$PYTHONPATH;	\
	cd test; pytest . 
========================================================================================== test session starts ==========================================================================================
platform darwin -- Python 3.10.11, pytest-8.0.2, pluggy-1.4.0
rootdir: /Users/dawood/git/data-prep-lab/transforms/universal/noop/test
collected 3 items                                                                                                                                                                                       

test_noop.py ..                                                                                                                                                                                   [ 66%]
test_noop_launch.py .                                                                                                                                                                             [100%]

========================================================================================== 3 passed in 17.15s ===========================================================================================
$
```
Note that the venv was activated for you as part of running the test.


