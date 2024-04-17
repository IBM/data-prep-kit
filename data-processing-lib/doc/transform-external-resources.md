# Support for external resources

Often when implementing a transform, the transform will require loading its own resources 
(e.g. models, configuration, etc.) to complete its job.  For example, the Blocklist transform
loads a list of domains to block.  Resources can be loaded from either S3 or local storage or a 
custom location defined by the transform (i.e. hugging face, etc).
In addition to actually loading the resource(s), the transform needs to define the configuration that 
defines the location of the domain list. 

In the next sections we cover the following:
   1. How to define the transform-specific resource location(s) as command line arguments
   2. How to load the transform-specific resources, either or both of:
      1. During transform initialization - this is useful for testing outside of ray, and optionally
      2. During transform configuration in the Ray orchestrator.  This may not be feasible if a resource 
         is not picklable.


## Defining Transform-specific Resource Locations 

Each transform has a _configuration_ class that defines the command line options with which the
transform can be configured.  In the example below, the [DataAccessFactory](../src/data_processing/data_access/data_access_factory.py)
 is used in the _configuration_ to add transform-specific arguments that allow a `DataAccessFactory` to be
initialized specifically for the transform.  The initialized `DataAcessFactory` is then made available to
the transform's initializer to enable it to read from transform-specific location.  Note that
you may choose not to use the DataAccessFactory and might have your own mechanism for loading a 
resource (for example, to load a hugging face model).  In this case you will define CLI arguments
that allow you to configure where the resources is located.
The implementation using DataAccessFactory looks as follows (the code here is from
[block listing](../../transforms/universal/blocklist/src/blocklist_transform.py)):

```python
class BlockListTransformConfiguration(DefaultTransformConfiguration):
    ...
    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the BlockListTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        
        ...
        
       # The DataAccess created by the DataAccessFactory below will use this CLI arg value 
       parser.add_argument(
            f"--{blocked_domain_list_path_key}",
            type=str,
            required=False,
            default=blocked_domain_list_path_default,
            help="S3/COS URL or local folder (file or directory) that points to the list of block listed domains."
        )  
        # Create the DataAccessFactor to use CLI args with the given blocklist prefix.
        self.daf = DataAccessFactory(f"{arg_prefix}_")
        # Add the DataAccessFactory parameters to the transform's configuration parameters.
        self.daf.add_input_params(parser)
```
We are creating an the `DataAccessFactory` using
a transform-specific prefix to define the transform-specific command line options to configure the 
transform's factory instance.
In this case, all the transform's DataAccessFactory parameters are prepended with 
`blocklist_`, (`arg_prefix`).  For example `blocklist_s3_cred`.

After configuring the command line argument parser above, 
The BlocklistConfiguration `apply_input_params()` is implemented to capture all 
`blocklist_` prefixed parameters and apply the arguments  to the DataAccessFactory.
In addition, it adds the factory to the parameters that will be made available
to the transform. In this way, the transform initializer will receive the DataAccessFactory
created and initialized by the configuration instance.

```python
    def apply_input_params(self, args: argparse.Namespace) -> bool:
        # Capture the args that are specific to this transform
        ...
        
        # Add the DataAccessFactory to the transform's configuration parameters.
        self.params[block_data_factory_key] = self.daf
        # mark this parameter to be removed
        self.remove_from_metadata.append(block_data_factory_key)
        # Validate and populate the transform's DataAccessFactory 
        return self.daf.apply_input_params(args)
```
Note here, that as daf can contain secrets we do not want him to show up in the execution metadata, we add its key
to the `self.remove_from_metadata` array. All the keys contained in this array will be removed from metadata. This 
can also be very usefull for any keys cantaining sensitive information, for example, secrets, passwords, etc.

The above code can be run in a non-ray main() as follows: 
```python
if __name__ == "__main__":
    parser = ArgumentParser()
    bltc = BlockListTransformConfiguration()
    bltc.add_input_params(parser)
    args = parser.parse()
    config = bltc.apply_input_params(args) 
    transform = BlockListTransform(config)
    ...
```

## When and Where to Load the Additional Resources 

With a DataAccessFactory established, it can be used in either the transform's Runtime class
when running in Ray, or in the transform's initializer to load the resource(s).
These two approaches have the following considerations:

* Loading in transform itself: 
    * Advantages
        * enables debugging without the need for a remote debugger to attach to the Ray worker.
        * simplifies local testing, especially if a transform itself can be tested locally.
        * can load any resource regardless of its picklability (irrelevant for data, but relevant for models).
    * Disadvantages
        * can create additional load on external resources, for example S3 or external web site.
* Loading in the Ray runtime, storing it plasma (Ray object storage), delivering it to the 
transform via pointer:
    * Advantages
        * minimises load on external resources, for example S3 or external web site
    * Disadvantages
        * can be problematic if the resource/model is not picklable
        * makes it slightly more complex for testing as loading is done in a process separate from the launcher

With the above in mind, we recommend at least loading the resource(s) in the transform's initializer.
This will ease debugging.  If load is an issue and the resource is picklable, then ALSO implement
loading in the transform's Runtime.
Next we show how to load resources using both approaches.

### Loading in the Transform Initializer 

If you decide to implement resource loading in the transform itself,
you can do this in the init method of the transform class. 
Let's look at the implementation, based on
[block listing](../../transforms/universal/blocklist/src/blocklist_transform.py)) example. The code below demonstrates loading of data.

```python
    # Get the DataAccessFactory we created above in the configuration
    daf = config.get(block_data_factory_key)
    if daf is None:
        raise RuntimeError(f"Missing configuration value for key {block_data_factory_key}")
    data_access = daf.create_data_access()
    url = config.get(blocked_domain_list_path_key)
    if url is None:
        raise RuntimeError(f"Missing configuration value for key {blocked_domain_list_path_key}")
    domain_list = get_domain_list(url, data_access)
```
Note that alternatively, 
if you are downloading data/models from the same source as the data itself,
you can use `data_access` key to get the DataAccess object used to read/write the data.

### Loading in the Transform Runtime

If you decide to implement resource loading in the transform runtime, 
you must implement a custom transform runtime class, in particular, to 
implement the `get_transform_config()` method that produces the configuration
for the transform an in this example, load the domain list and store a Ray reference in the configuration. 
Let's look at the implementation, based on the [block listing](../../transforms/universal/blocklist/src/blocklist_transform.py)) transform. 
First define the initializer() which must accept a dictionary of parameters as generally
will be defined by the configuration and its CLI parameters.

```python
class BlockListRuntime(DefaultTableTransformRuntime):

    def __init__(self, params: dict[str, Any]):
        super().__init__(params)

```
Once this is done, you need to at least implement `get_transform_config` method,
which is called by the Ray orchestrator to establish the transform configuration parameters
passed to the Ray Worker that then creates the transform instance using the provided 
configuation parameters.  In short, these are the parameters that will be used to configure
your transform in the Ray worker.

```python
    def get_transform_config(
        self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        # create the list of blocked domains by reading the files at the conf_url location
        url = self.params.get(blocked_domain_list_path_key, None)
        if url is None:
            raise RuntimeError(f"Missing configuration key {blocked_domain_list_path_key}")
        blocklist_data_access_factory = self.params.get(block_data_factory_key, None)
        if blocklist_data_access_factory is None:
            raise RuntimeError(f"Missing configuration key {block_data_factory_key}")
        # Load domain list 
        domain_list = get_domain_list(url, blocklist_data_access_factory.create_data_access())
        # Store it in Ray object storage
        domain_refs = ray.put(domain_list)
        # Put the reference in the configuration that the transform initializer will use.
        return {domain_refs_key: domain_refs} | self.params
```
In the implementation above, we do something very similiar to what was done in the transform
initializer, except that here we store the loaded resource (i.e. the domain list) in Ray global memory
and place the key for the stored object in the configuration.  This way the transform initializer
can first look for this key and if found, avoid loading the domain list itself.

Alternatively, if you are downloading resources from the **same** source as 
the data itself, you can use input parameter `data_access_factory` to create data access 
and download everything that you need using it.

Finally, and as mentioned above, the transform's initializer looks to see if the key
to the domain list is present and uses it instead of loading the domain list itself.

```python
        runtime_provided_domain_refs = config.get(domain_refs_key, None)
        if runtime_provided_domain_refs is None:
            # this is useful during local debugging and testing without Ray
            url = config.get(blocked_domain_list_path_key, None)
            if url is None:
                raise RuntimeError(f"Missing configuration value for key {annotation_column_name_key}")
            daf = config.get(block_data_factory_key, None)
            if url is None:
                raise RuntimeError(f"Missing configuration value for key {block_data_factory_key}")
            data_access = daf.create_data_access()
            domain_list = get_domain_list(url, data_access)
        else:
            # This is recommended for production approach. In this case domain list is build by the
            # runtime once, loaded to the object store and can be accessed by actors without additional reads
            try:
                domain_list = ray.get(runtime_provided_domain_refs)
            except Exception as e:
                logger.info(f"Exception loading list of block listed domains from ray object storage {e}")
                raise RuntimeError(f"exception loading from object storage for key {runtime_provided_domain_refs}")
```
### Conclusion

Generally, although both resource loading approaches can be used, we recommend 
always implementing loading in the transform initializer, and if desired, loading in the transform 
runtime if feasible (picklable, etc) or desirable for other reasons such as network bandwidth.
