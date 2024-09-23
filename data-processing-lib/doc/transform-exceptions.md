# Exceptions
A transform may find that it needs to signal error conditions.
For example, if a referenced model could not be loaded or
a given input data (e.g., pyarrow Table) does not have the expected format (.e.g, columns).
In general, it should identify such conditions by raising an exception. 
With this in mind, there are two types of exceptions:

1. Those that would not allow a specific datum to be processed (e.g. missing column).
2. Those that would not allow any data to be processed (e.g. model loading problem).

To fail on an individual piece of data (1 above) provided to either
AbstractBinaryTransform's `transform_binary`, or
AbstractTableTransform's tranform()`,
these methods should throw any exception other than UnrecoverableException. 
This will cause only the error-causing datum to be ignored and not written out,
but allow continued processing of data by the transform.

To terminate processing of all data by the runtime (2 above), 
the transform should either 
* throw any exception in `__init__()`, or 
* throw an UnrecoverableException from AbstractBinaryTransform's `transform_binary`, `flush_binary(), or
from AbstractTableTransform's tranform()` or `flush().

In both cases, the runtime will log the exception as an error.
