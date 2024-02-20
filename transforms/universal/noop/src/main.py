
if __name__ == "__main__":
    # Simply instantiate the driver and initialize with mutator specifics
    launcher = TransformLauncher(
        # The name of the mutator to be using in logging and others.
        "NOOP",
        # A companion class for the mutator that runs in the driver to provide optional
        # CLI args for the mutator configuration and combines the optional metadata produced by the mutator.
        NOOPTransformRuntime,
    )

    # Call the drivers main() method to create the ray actors and have them call the mutator on Tables read from parquet files.
    # Run this with --help to see the full set of available generic and mutator-specific CLI args.
    launcher.execute()
