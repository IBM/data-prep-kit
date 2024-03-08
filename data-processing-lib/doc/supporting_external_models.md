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
to the dictionary of transform parameters. The implementation of such approach looks as follows:

```python

```

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

### Loading data in Transform init

### Loading data in Transform runtime

Generally, although both data loading approaches can be used for implementation, we recommend loading models/data
in transform runtime if it is possible (picklability or size issues).