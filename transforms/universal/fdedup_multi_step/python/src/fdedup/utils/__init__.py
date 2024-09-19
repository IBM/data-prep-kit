from fdedup.utils.compute_shingles import compute_shingles
from fdedup.utils.fdedup_classes import (NO_SIMILARITY,
                                         MurmurMH,
                                         DocCollector,
                                         DocsMinHash,
                                         BucketsHash,
                                         BucketsHashProcessor
                                         )
from fdedup.utils.fdedupsupport import  FdedupSupport, LONG_BUCKET, REQUEST_LEN
