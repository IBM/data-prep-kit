# Exceptions
A transform may find that it needs to signal error conditions.
For example, if a referenced model could not be loaded or
a given input data (e.g., pyarrow Table) does not have the expected format (.e.g, columns).
In general, it should identify such conditions by raising an exception. 
With this in mind, there are two types of exceptions:

1. Those that would not allow any data to be processed (e.g. model loading problem).
2. Those that would not allow a specific datum to be processed (e.g. missing column).

In the first situation the transform should throw an 
[unrecoverable exception](../python/src/data_processing/utils/unrecoverable.py), which
will cause the runtime to terminate processing of all data. 
**Note:** any exception thrown from `init` method of transform will cause runtime to 
terminate processing

In the second situation (identified in the `transform()` or `flush()` methods), the transform
should throw an exception from the associated method.
This will cause only the error-causing datum to be ignored and not written out,
but allow continued processing of tables by the transform.
In both cases, the runtime will log the exception as an error.