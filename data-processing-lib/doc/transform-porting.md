# Transform Porting
We make here some small points about porting from GUF 0.1.6.

1. AbstractTableMutator class is replaced by AbstractTableTransform, but largely has similary function
    * transform() now returns a **list** of Tables instead of just one.
2. AbstractMutatorRuntime class is replaced by DefaultTableTransformConfiguration
    * add_mutator_cli_args() is replaced by add_input_params() but provides the same function
    * combine_metadata() method is no longer needed/used.
    * apply_input_params() must be added to capture transform-specific arguments and validate them.
3. MutatingRayDriver is replaced with TransformLauncher
    * Initialize TransformLauncher with the transform's configuration class which extends DefaultTableTransformConfiguration 
    * Call launch() instead of main().
   


GUF 0.1.6 NOOPMutator
```python
class NOOPMutator(AbstractTableMutator):

    def __init__(self, config: dict):
        super().__init__(config)
        self.sleep_msec = config.get("noop_sleep_msec", None)

    def mutate(self, table: pyarrow.Table) -> tuple[pyarrow.Table, dict]:
        if self.sleep_msec is not None:
            print(f"Sleep for {self.sleep_msec} milliseconds")
            time.sleep(self.sleep_msec / 1000)
            print("Sleep completed - continue")
        rows = len(table)
        metadata = {"nrows": rows, "nfiles": 1}
        return table, metadata
```

New Framework NOOPTransform 
```python
class NOOPTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        self.sleep = config.get("sleep", 1)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict[str, Any]]:
        if self.sleep is not None:
            print(f"Sleep for {self.sleep} seconds")
            time.sleep(self.sleep)
            print("Sleep completed - continue")
        metadata = {"nfiles": 1, "nrows": len(table)}
        return [table], metadata

```


GUF 0.1.6 NOOPMutatorRuntime
```python
class NOOPMutatorRuntime(AbstractMutatorRuntime):

    def add_mutator_cli_args(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--noop_sleep_msec",
            type=int,
            required=False,
            help="Sleep actor for a number of milliseconds while processing the data frame, before writing the file to COS",
        )
        return parser

    def combine_metadata(self, m1: dict, m2: dict) -> dict:
        m1_files = m1["nfiles"]
        m2_files = m2["nfiles"]
        m1_rows = m1["nrows"]
        m2_rows = m2["nrows"]
        combined = {"nrows": m1_rows + m2_rows, "nfiles": m1_files + m2_files}
        return combined
```


New Framework NOOPTransformConfiguration
```python
class NOOPTransformConfiguration(DefaultTableTransformConfiguration):
    def __init__(self):
        super().__init__(name="NOOP", runtime_class=DefaultTableTransformRuntime, transform_class=NOOPTransform)
        self.params = {}

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--noop_sleep_sec",
            type=int,
            default=1,
            help="Sleep actor for a number of seconds while processing the data frame, before writing the file to COS",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        if args.noop_sleep_sec <= 0:
            print(f"Parameter noop_sleep_sec should be greater then 0, you specified {args.noop_sleep_sec}")
            return False
        self.params["sleep"] = args.noop_sleep_sec
        print(f"noop parameters are : {self.params}")
        return True


```