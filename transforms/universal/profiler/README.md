# Profiler Transform 

Profiler implement a word count. Typical implementation of the word count is done using map reduce.
* It’s O(N2) complexity
* shuffling with lots of data movement

Implementation here is using “streaming” aggregation, based on central cache:

* At the heart of the implementation is a cache of partial word counts, implemented as a set of Ray actors and containing
  word counts processed so far.
* Individual data processors are responsible for:
    * Reading data from data plane
    * tokenizing documents (we use pluggable tokenizer)
    * Coordinating with distributed cache to collect overall word counts

The complication of mapping this model to transform model is the fact that implementation requires an aggregators cache,
that transform mode knows nothing about. The solution here is to use transform runtime to create cache
and pass it as a parameter to transforms.

