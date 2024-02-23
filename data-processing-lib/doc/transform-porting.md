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