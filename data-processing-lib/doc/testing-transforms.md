# Testing Transforms
A test framework is provided as part of the library in the `data_processing_test` package.
Tranform testing makes use of an `AbstractTransformTest` class that defines 
the generic tests that would be common to all transforms.
Initially this means testing the transformation of one or more in-memory 
input tables to a one or more output tables and one or more metadata dictionaries.

The `AbstractTransformTest` class defines the `test_*(...)` methods and makes use 
of pytest fixtures to define test cases (i.e. inputs) applied to the test method(s).
Each `test_*(...)` method (currently only one) has an associated abstract
`get_test_*_fixtures()` method that must be implemented by the specific
transform implementation test to define the various sets of inputs tested.
For example,

```python
def pytest_generate_tests(metafunc):
    test_instance = metafunc.cls()  # Create the instance of the test class being used.
    test_instance.install_fixtures(metafunc)  # Use it to install the fixtures

```

In addition, to support the installation of the pytest fixtures, a 
`pytest_generate_tests(metafunc)` function is defined in each transform's
test file.  The definition of this function is generally the same for all 
