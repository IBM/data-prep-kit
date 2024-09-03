from fdedup.utils.compute_shingles import compute_shingles
from fdedup.utils.support_classes import (fuzzy_optimal_param,
                                          jaccard_distance,
                                          process_buckets_locally,
                                          merge_doc_ids,
                                          remove_docs_buckets,
                                          NO_SIMILARITY,
                                          MurmurMH,
                                          DocCollector,
                                          DocsMinHash,
                                          BucketsHash,
                                          BucketsHashProcessor
                                          )
