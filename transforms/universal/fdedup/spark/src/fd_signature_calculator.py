import json
import logging
import os

import mmh3
import numpy as np
from Murmur_MH import Murmur_MH
from pyspark import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from scipy.integrate import quad as integrate
from spark_transformer_runtime import SparkFileBatcher, SparkTransformerRuntime


def _optimal_minhashlsh_param(
    threshold: float = 0.8, num_perm: int = 128, false_positive_weight: float = 0.5, false_negative_weight: float = 0.5
):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative.
    :param threshold: desired similarity threshold
    :param num_perm: number of permutations
    :param false_positive_weight: importance of avoiding false positive results
    :param false_negative_weight: importance of avoiding false negative results
    :return: a tuple (optimal number of bands, optimal number of rows)
    """

    def _false_positive_probability(threshold, b, r):
        _probability = lambda s: 1 - (1 - s ** float(r)) ** float(b)
        a, err = integrate(_probability, 0.0, threshold)
        return a

    def _false_negative_probability(threshold, b, r):
        _probability = lambda s: 1 - (1 - (1 - s ** float(r)) ** float(b))
        a, err = integrate(_probability, threshold, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = _false_positive_probability(threshold, b, r)
            fn = _false_negative_probability(threshold, b, r)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


class FDSignatureCalculator(SparkTransformerRuntime):
    """
    This class takes as input a data set with two mandatory columns (document ID and contents),
    and outputs two data sets: (1) a set of `num_permutations' minhashes for each document, and
    (2) a set of <hash, band, document_id, document_length> records.
    To generate the first data set, word shingles of a specific length are generated,
    then minhashes are calculated using those shingles. Subsequently, 'r' groups of the minhashes
    are hashed in 'b' bands to generate the second data set.
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        file_ext: str,
        default_batch_size: int,
        document_id_column: str,
        contents_column: str,
        num_permutations: int,
        word_shingle_size: int,
        jaccard_similarity_threshold: int,
        debug: bool,
        language: str,
        checkpoint_count: int,
        total_document_size: int,
        total_number_of_documents: int,
        seed: int,
        configs: str,
    ):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path
        self.minhash_output_path = os.path.join(self.output_path, "minhashes")
        self.band_output_path = os.path.join(self.output_path, "bands")
        self.file_ext = file_ext
        self.default_batch_size = default_batch_size
        self.document_id_column = document_id_column
        self.contents_column = contents_column
        self.num_permutations = num_permutations
        self.word_shingle_size = word_shingle_size
        self.jaccard_threshold = jaccard_similarity_threshold
        self.debug = debug
        self.language = language
        self.checkpoint_count = checkpoint_count
        self.total_document_size = total_document_size
        self.total_number_of_documents = total_number_of_documents
        self.seed = seed
        self.configs = configs
        self.init_io(self.input_path, self.output_path)
        server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
        if server_port_https == -1:
            # if the code is running locally, add Murmur_MH.py to the py files used by the Spark context
            murmur_mh_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Murmur_MH.py")
            self.spark.sparkContext.addPyFile(murmur_mh_filepath)
        logging.info("Initialized Spark Fuzzy Dedupe")

    def create_salt_contents_rdd(self, spark_df: DataFrame, contents_column: str) -> RDD:
        """
        Calculates salt for each document, create a dataframe column with a
        structure containing the contents and the salt, selects the document ID
        and contents_salt column from the dataframe, and returns them as an RDD

        :param spark_df: the input dataframe
        :param contents_column: the dataframe column that contains the text
        :return: an RDD containing, for each row, the doc ID and contents_salt columns
        """
        if self.debug:
            logging.debug(f"DEBUG: sparkDF count: {spark_df.count()}")
        # load Resilient Distributed Datasets()
        resilient_distributed_dataset = spark_df.select(self.document_id_column, contents_column).rdd
        return resilient_distributed_dataset

    def generate_word_shingles_and_minhashes(
        self, resilient_distributed_dataset: RDD, mm_min_hash: Murmur_MH, language: str, word_shingle_size: int
    ) -> RDD:
        """
        Generate word shingles and minhashes - sequences of words with length 'word_shingle_size'.
        TODO: preprocess the text before generating the word shingles
        For example if word_shingle_size is 3, the document "This is a document"
        will generate the following: ["This is a", "is a document"]
        :param resilient_distributed_dataset: the input RDD with two mandatory columns - document_id and contents
        :param mm_min_hash: Murumur_MH instance used to calculate minhashes

        """

        # define shingles generation function
        def _generate_word_shingles(
            text: str, window_size: int = 5, delimiter: str = " ", language: str = "en"
        ) -> tuple[list, int]:
            """
            return a tuple
            (A, B)
                A = the list of k-word shingles
                B = number of characters in document
            """
            # TODO: for now, don't worry about Japanese language
            if language == "jp":
                # sp = sentencepiece.SentencePieceProcessor()
                # model_path = "./ja.sp.model"
                # sp.load(model_path)
                # try:
                #     import text_normalizer
                #     text = text_normalizer.normalize(text)
                # except ImportError:
                #     text = text

                # words = sp.encode_as_pieces(text)
                ...
            else:
                words = text.split()

            doc_len = len(text)
            # print(len(words), ' and ', window_size)
            word_count = len(words)
            k_shingles = []
            # print( max(1, len(words) - window_size + 1))
            for i in range(0, max(1, word_count - window_size + 1)):
                # get a shingle of words and join with delimiter
                # yield delimiter.join(words[i:min(i+window_size, word_count)])
                k_shingles.append(delimiter.join(words[i : i + window_size]))
            return k_shingles, doc_len

        # Generate word shingles
        logging.info(" Generate word shingles and minhashes")
        minhashed = resilient_distributed_dataset.mapValues(
            lambda text: mm_min_hash.minhash2_nosalt(*_generate_word_shingles(text, window_size=word_shingle_size))
        )
        if self.debug:
            logging.info(f"minhashed has {minhashed.count()} rows")
            logging.info(f"{minhashed.take(1)}")
        return minhashed

    def calculate_bands(
        self,
        minhashed: RDD,
        minhashlsh_num_bands: int,
        minhashlsh_length_band: int,
    ) -> RDD:
        """
        Calculate the band hashes from the minhash signatures
        :param minhashed: RDD that contains the minhash signatures for each document
        :param minhashlsh_num_bands: number of bands
        :param minhashlsh_length_band: number of rows per band
        :return: an RDD, containing the band hashes, the band indexes, doc IDs, and doc lengths
        """

        def emit_bands2(doc_id: str, minhashes: np.array, b: int, r: int, seed: int = 42):
            """
            Return

            [a1, a2, a3, ..., a_n] = np.array of minhashes, we calculate
            a1, a2,...a[r] --> band-hash based on r minhashes
            a[r+1],...,a[2r] --> band-hash based on r minhashes
            ...
            a[(b-1)r+1],...,a[b*r]
            """
            num_minhashes = len(minhashes)
            assert (
                b * r <= num_minhashes
            ), f"b*r must be <= num minhashes, was b={b}, r={r}, num_minhashes={num_minhashes}"

            for band_index in range(b):
                # we create the band hashes by hashing the slice of minhashes
                band_hash = mmh3.hash64(minhashes[band_index * r : (band_index + 1) * r], seed=seed, signed=True)[0]
                yield (band_hash, band_index, doc_id)

        bands = minhashed.flatMap(
            lambda x: emit_bands2(
                doc_id=x[0],
                minhashes=np.array(x[1][0]).astype(np.uint32),
                b=minhashlsh_num_bands,
                r=minhashlsh_length_band,
            )
        )
        if self.debug:
            logging.info(f"bands has {bands.count()} rows")
            logging.info(f"{bands.take(1)}")
        return bands

    def run_transform(self):
        in_out_metadata = {}
        self.input_files, self.file_stats = self.list_files(self.input_path, self.file_ext)
        file_batcher = SparkFileBatcher(
            self.input_files,
            self.default_batch_size,
            self.out_data_access,
            os.path.join(self.output_path, "checkpoint.txt"),
        )
        logging.info("Starting signature calculation process")

        # instantiate once on driver so that every worker will use the same hash functions
        mm_min_hash = Murmur_MH(num_perm=self.num_permutations, seed=self.seed)

        #  Compute the optimal `MinHashLSH` parameters
        logging.info(" Compute the optimal `MinHashLSH` parameters")
        minhashlsh_num_bands, minhashlsh_length_band = _optimal_minhashlsh_param(
            threshold=self.jaccard_threshold,
            num_perm=self.num_permutations,
            false_positive_weight=0.5,
            false_negative_weight=0.5,
        )

        # Define the minhash schema
        minhash_schema = StructType(
            [
                StructField(self.document_id_column, LongType(), True),
                StructField(
                    "data",
                    StructType(
                        [
                            StructField("minhashes", ArrayType(IntegerType()), True),
                            StructField("document_length", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

        band_schema = StructType(
            [
                StructField("band_hash", LongType(), True),
                StructField("band_index", IntegerType(), True),
                StructField(self.document_id_column, LongType(), True),
            ]
        )

        while True:
            self.checkpoint_count += 1
            # Get next batch
            input_batch, batch_size = file_batcher.next_batch()
            # used to track document size
            self.total_document_size += batch_size
            if not input_batch:
                break

            logging.info(
                f"Processing batch {file_batcher.current_batch_index} " f"out of {file_batcher.total_batches}"
            )
            logging.debug(input_batch)
            spark_df = self.read_data(input_batch, self.file_ext)
            self.total_number_of_documents += spark_df.count()
            resilient_distributed_dataset = self.create_salt_contents_rdd(
                spark_df,
                self.contents_column,
            )

            # generate word shingles and minhashes
            minhashed = self.generate_word_shingles_and_minhashes(
                resilient_distributed_dataset,
                mm_min_hash,
                self.language,
                self.word_shingle_size,
            )
            minhashed_df = self.spark.createDataFrame(minhashed, schema=minhash_schema)
            self.write_data(minhashed_df, self.minhash_output_path, data_type="parquet")
            logging.info(f"Saved minhashed_df to {self.output_path}")
            logging.info(f"DEBUG: minhashed count: {minhashed.count()}")

            # calculate bands
            logging.info(" Calculate bands")
            bands = self.calculate_bands(
                minhashed,
                minhashlsh_num_bands=minhashlsh_num_bands,
                minhashlsh_length_band=minhashlsh_length_band,
            )
            bands_df = self.spark.createDataFrame(
                bands,
                schema=band_schema,
            )
            num_bands_partitions = bands.getNumPartitions()
            in_out_metadata["num_minhash_bands"] = num_bands_partitions
            in_out_metadata["num_bands"] = minhashlsh_num_bands

            for band_ix in range(minhashlsh_num_bands):
                band_df = bands_df.filter(F.col("band_index") == band_ix).drop("band_index")
                self.write_data(band_df, os.path.join(self.band_output_path, f"band_{band_ix}"), data_type="parquet")
            logging.info(f" Bands calculation complete: number of bands partitions = {num_bands_partitions}")

        # Convert JSON to Row
        row_data = Row(**in_out_metadata)

        # Create DataFrame
        df = self.spark.createDataFrame([row_data])
        self.write_data(df, os.path.join(self.band_output_path, "metadata"), data_type="json")

    def execute(self):
        try:
            self.run_transform()
            logging.info("Finished generating document signatures")
        except Exception as ex:
            logging.error(f"Failed to generate document signatures: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
