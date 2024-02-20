from data_processing.ray.launcher import TransformLauncher
from noop import NOOPTransformRuntimeFactory


if __name__ == "__main__":
    # Simply instantiate the driver and initialize with mutator specifics
    launcher = TransformLauncher("NOOP", NOOPTransformRuntimeFactory())

    # Call the drivers main() method to create the ray actors and have them call the mutator on Tables read from parquet files.
    # Run this with --help to see the full set of available generic and mutator-specific CLI args.
    launcher.launch()
