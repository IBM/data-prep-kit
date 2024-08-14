## Background: Fuzzy Dedup Flow Overview

Fuzzy dedup transform is a pipeline with four steps, and each step runs in a new Spark cluster:
* Signature calculation (word shingles, min hashes, band hashes)
* Document Cluster calculation (per band)
* Identification of doc ids to remove (for the entire data set)
* Removal of the duplicate documents

### Signature Calculation
Takes as input the original data set, and outputs the minhashes, band hashes and document size for each document. The output generated for one document in one band (`n` = number of permutations) has the following format:
```
<band_hash,doc_id,doc_size,[mh_1,…, mh_n]>
```

The output has a fixed size per document:
```
size(uint64) + size(uint64) + size(uint64) + n * size(uint32)
```

If `n = 64 (128)`, one document takes `280 (536)` bytes. A data set of ~500GB with ~280 million documents will have a signature space of 78.4GB (150GB) for n = 64 (128), while a data set of ~45TB with ~5.3 billion documents will take 1.48TB (2.84TB) for n = 64 (128).  

### Cluster Calculation
Takes as input the signatures computed in the previous step and, for each band, groups the documents by their band hash. For each band, group docs by their band hash
To scale and parallelize this step:
* Divide the hash interval `[0, max]` in `n` segments
* Save the hashes that fall in the same segment in the same location, different from the location of the other segments
* Run each segment in parallel

### Calculation of List of Document IDs to Remove
Iterate through the list of clusters for each band and build a list of doc IDs to remove. Take the union of the lists of doc IDs to remove from each, and keep the distinct values from this union.

### Removal of Duplicate Documents

Pass the list of doc IDs to remove to each worker. Use the original data set and the list of doc IDs to remove to filter out all duplicates.

### Runtime Requirements for Fuzzy Dedup
* Capability to save output to several locations, based on the value of the band hash. Provide the following file structure for signatures:
   ```
   signatures/band_0/segment_0
   signatures/band_0/segment_1
   signatures/band_1/segment_0
   signatures/band_1/segment_1
   ```
* Capability to handle in parallel the files from each one of these folders
* Capability to run each step of fuzzy dedup in a new Spark cluster
