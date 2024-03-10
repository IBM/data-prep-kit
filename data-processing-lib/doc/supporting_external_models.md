# Support for external models

A common way of implementing transformers is leveraging custom models (or additional data) for implementation
of the transforms. There are several complications in such implementations: 
* additional models/data can be located in different places, including:
  * local data - models/data is baked in the docker image
  * S3 bucket different from the bucket where actual data for transformation cn be located
  * etc
* loading of the additional data/models can technically be done in two different places:
  * in transform itself, during initialization of transform
  * In the transform runtime 

Lets discuss in details how to overcome these complications

## Sources of additional models/data 

To overcome issues with additional data sources, a simplest and recommended way is to create 
additional `data access factory` as part of transform specific configuration that can be added
to the dictionary of transform parameters. The implementation of such approach looks as follows
(the code here is from [block listing](../../transforms/universal/blocklisting/src/blocklist_transform.py)):

```python
    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the BlockListTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{blocked_domain_list_path_key}",
            type=str,
            required=False,
            default=blocked_domain_list_path_default,
            help="COS URL or local folder (file or directory) that points to the list of block listed domains.  "
            "If not running in Ray, this must be a local folder.",
        )
        parser.add_argument(
            f"--{annotation_column_name_key}",
            type=str,
            required=False,
            default=annotation_column_name_default,
            help="Name of the table column that contains the block listed domains",
        )

        parser.add_argument(
            f"--{source_url_column_name_key}",
            type=str,
            required=False,
            default=source_column_name_default,
            help="Name of the table column that has the document download URL",
        )
        # Add block-list-specific arguments to create the DataAccess instance to load the domains.
        self.daf = DataAccessFactory(f"{self.name}_")
        self.daf.add_input_params(parser)
```
Note that at the end of this snipet, we are creating an additional DataAccess factory and exposing its configuration. 
All of the parameters, in this case are prepended with with the `blocklist_`.
Once this is done, an `apply_input_params` below can build its own `data_access_factory` and store it into parameters 
that can be then delivered to both transform runtime and transform itself. The code doing it is below:

```python
    def apply_input_params(self, args: argparse.Namespace) -> bool:
        arg_keys = [blocked_domain_list_path_key, annotation_column_name_key, source_url_column_name_key]
        dargs = vars(args)
        for arg_key in arg_keys:
            # Make sure parameters are defined
            if dargs.get(arg_key) is None or len(dargs.get(arg_key)) < 1:
                logger.info(f"parameter {arg_key} is not defined, exiting")
                return False
            self.params[arg_key] = dargs.get(arg_key)
        self.params[block_data_factory_key] = self.daf
        # populate data access factory
        return self.daf.apply_input_params(args)
```
The only caveat of this approach that by default, all of the parameter from the dictionary is copied to the metadat, 
which throws an exception, as `data_access_factory` is not convertable to JSON. To pevent this from hapenning, 
the following additional method needs to be implemented:

```python
    def get_transform_metadata(self) -> dict[str, Any]:
        del self.params[block_data_factory_key]
        return self.params
```
When returning configuration parameters for metadata usage, this method remove `data_access_factory` from the 
parameters list. thus creating metadata, that can be converted to JSON.

## Placing of loading of the additional data/models

When deciding where to loading additional models/data the following needs to be considered
* Loading in transform itself has the following advantages and disadvantages:
    * Advantages
        * simplifies local testing, especially if a transform itself can be tested locally
        * can load any data regardless of its picklability (irrelevant for data, but relevant for models)
    * Disadvantages
        * can create additional load on external resources, for example S3 or external web site 
* Loading in runtime and then storing it plasma (Ray object storage) and delivering it to the transform via pointer has the following advantages and disadvantages:
    * Advantages
        * minimises load on external resources, for example S3 or external web site
    * Disadvantages
        * can be problematic if the model is not picklable
        * makes it slightly more complex for local testing, which can be easily overcome - see below

Here we will show how to load data/model using both approaches 

### Loading data in Transform runtime

If you decide to implement model/data loading in the transform runtime, you have to implement custom transform runtime 
class. Lets look at the implementation, based on 
[block listing](../../transforms/universal/blocklisting/src/blocklist_transform.py)) example. The code below demonstrates definition and implementation of the class.

```python
class BlockListRuntime(DefaultTableTransformRuntime):

    def __init__(self, params: dict[str, Any]):
        super().__init__(params)

```
Once this is done, you need to at least implement `get_transform_config` method

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

        domain_list = get_domain_list(url, blocklist_data_access_factory.create_data_access())
        domain_refs = ray.put(list(domain_list))
        return {domain_refs_key: domain_refs} | self.params
```
In the implementation above, we create data/model specific data access based on the data access factory, that we have 
configured, get the domain list, sore it to the Ray object storage and add the data storage key to the transform 
parameters.

Note that in the example above we are using an additional data access factory `blocklist_data_access_factory`, 
that we described above. Alternatively, if you downloading data/models from the same source as the data itself, 
you can use input parameter `data_access_factory` to create data access and download everything that you 
need using it.

And the final step here is to get data/model in the transform init and store it locally:

```python
        runtime_provided_domain_refs = config.get(domain_refs_key, None)
        domain_list = ray.get(runtime_provided_domain_refs)
```

### Loading data in Transform init

If you decide to implement model/data loading in the transform iteself, you can do this in the init method of transform
class. Lets look at the implementation, based on
[block listing](../../transforms/universal/blocklisting/src/blocklist_transform.py)) example. The code below demonstrates loading of data.

```python
            daf = config.get(block_data_factory_key, None)
            if url is None:
                raise RuntimeError(f"Missing configuration value for key {block_data_factory_key}")
            data_access = daf.create_data_access()
            domain_list = get_domain_list(url, data_access)
```

Note that here, similar to the example above we are using an additional data access factory `blocklist_data_access_factory`,
that we described above. Alternatively, if you downloading data/models from the same source as the data itself,
you can use global `data_access` (it is also in the config with the key "data_access") to download everything that you
need using it.

### Using both loading methods for local testing

As we mentioned above, loading data in Transform runtime has one disadvantage - it does not provide capability
for local testing. To overcome this drawback, you can implement transform `init()` method, that tries to get 
`domain_list` (using blocklisting example again), but if it fails, then it tries to download it locally. The code 
below shows the implementation:

```python
        runtime_provided_domain_refs = config.get(domain_refs_key, None)
        if runtime_provided_domain_refs is None:
            # this is only useful during local debugging without Ray
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

Generally, although both data loading approaches can be used for implementation, we recommend loading models/data
in transform runtime if it is possible (picklability or size issues).