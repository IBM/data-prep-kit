# Fuzzy Dedup

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

The basic implementation of the fuzzy dedup is based on [MinHash](https://en.wikipedia.org/wiki/MinHash). Also see
[here](http://infolab.stanford.edu/~ullman/mmds/ch3n.pdf) for more details. 

This version of fuzzy dedup code is split into 3 transforms:
* Fuzzy dedup preprocessor is responsible for reading of the original data, computing 
  minhashes for the individual documents (table rows) and building LSH buckets. The result 
  of this step is a set of snapshots (buckets and minhashes), that are used by the next steps 
  of fuzzy deduping
* Fuzzy dedup bucket processor is a workhorse of implementation, that is responsible for
  duplicates removing. This step starts from populating minhash cache (based on the snapshot)
  from the previous step and creating documents cache. It then reads files from buckets
  hash snapshot and process every bucket to remove duplicates. To allow for better scalability
  of this step, bucket processing is not done by the file readers, but rather by a dedicated
  bucket hash processor, that can be independently scaled. Unique documents IDs are collected 
  by doc ID cache and are snapshotted at the end of this step. Additionally this step produces
  minhash and bucket hash snapshot, which contain only non-duplicate documents. These two 
  snapshots can be used for implementation of the incremental fuzzy dedup (see below)
* Fuzzy dedup fillter is responsible for re reading of the original data and filtering out
  all duplicate documents based on the doc ID cache which is created based on the snapshot 
  produced in the previous step.

The above implementation is more flexible compared to the [original](../fdedup) as it allows for
individual scaling of steps and provides natural splitting of the application for easier partial
restarts. It also provides simpler, easier to maintain code.

Finally this implementation makes it trivial to implement incremental fuzzy dedup. Lets assume
that We have a data set A, that we can run from the fuzzy dedup steps. Now if we have a new 
data set B that we want to deduplicate along with set A, we ca do the following:
* Use [doc id transform](../doc_id) to populate unique documents integer IDs starting from the 
last ID used by data Set A.
* For preprocessing step of data set B start from the minhash/bucket hash caches populated from
the data set A snapshots. Note that in the minhash snapshot of data set A we need to set doc
length to very large numbers to ensure that they will not be removed
* Execute Fuzzy dedup bucket processor and Fuzzy dedup filter steps. The result will contain 
  deduplicated documents from both data sets.


