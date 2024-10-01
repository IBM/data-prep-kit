# SP Transform 
The SP transform performs the semantic profiling of code snippets in a dataset. This
is done based on the libraries and their categorization obtained from an Internal 
Knowledge Base (IKB) which is generated offline using LLMs.Per the set of 
[transform project conventions](../../README.md#transform-project-conventions)
the following runtimes are available:

* [python](python/README.md) - provides the base python-based transformation 
implementation.
* [ray](ray/README.md) - enables the running of the base python transformation
in a Ray runtime

