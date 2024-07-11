import functools
import json
import logging
import math
import os
from typing import Union

import disjoint_set
import mmh3
import numpy as np
from fd_signature_calculator import _optimal_minhashlsh_param
from Murmur_MH import Murmur_MH
from pyspark import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import array_sort, col, collect_list, first, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when
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


class FDClustersCalculator(SparkTransformerRuntime):
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
        salting_size: int,
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
        self.clusters_doc_ids_output_path = os.path.join(self.output_path, "clusters_doc_ids")
        self.file_ext = file_ext
        self.default_batch_size = default_batch_size
        self.document_id_column = document_id_column
        self.contents_column = contents_column
        self.num_permutations = num_permutations
        self.word_shingle_size = word_shingle_size
        self.jaccard_similarity_threshold = jaccard_similarity_threshold
        self.num_bands, _ = _optimal_minhashlsh_param(self.jaccard_similarity_threshold, self.num_permutations)
        self.salting_size = salting_size
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

    def filter_by_band_hash(self, df_a, df_b):
        # Perform an inner join on the band_hash column to filter rows in df_a that exist in df_b
        df_filtered = df_a.join(df_b, on="band_hash", how="inner").select(df_a["*"])
        return df_filtered

    def group_and_aggregate(self, df):
        # # Group by band_hash and aggregate int_id_column into a list and take the first length
        # df_grouped = df.groupBy("band_hash").agg(
        #     collect_list("int_id_column").alias("doc_ids")
        # )
        # Group by band_hash and collect list of int_id_column, then sort the list
        df_grouped = df.groupBy("band_hash").agg(collect_list("int_id_column").alias("doc_ids"))
        return df_grouped

    def aggregate_dataframes(self, df1, df2):

        # Union the two dataframes
        df_union = df1.union(df2)

        # Group by hash and sum the counts
        joined_df = df_union.groupBy("band_hash").agg(_sum("number_of_documents").alias("number_of_documents"))

        return joined_df

    def run_transform(self):

        spark_df_metadata = self.read_data(os.path.join(self.band_output_path, "metadata"), "json")
        in_out_metadata_val = spark_df_metadata.toJSON().collect()
        json_object = json.loads(in_out_metadata_val[0])
        num_bands = json.loads(json_object.get("value"))["num_bands"]

        # Define schema
        schema = StructType(
            [
                StructField("band_hash", LongType(), True),
                StructField("number_of_documents", IntegerType(), True),
            ]
        )

        # Create an initial empty DataFrame
        final_df = self.spark.createDataFrame([], schema)

        for band_index in range(num_bands):
            final_band_df = self.spark.createDataFrame([], schema)
            bands_path = os.path.join(self.band_output_path, f"band_{band_index}")
            self.input_files, self.file_stats = self.list_files(bands_path, self.file_ext)

            file_batcher = SparkFileBatcher(
                self.input_files,
                self.default_batch_size,
                self.out_data_access,
                os.path.join(self.output_path, "checkpoint.txt"),
            )
            logging.info("Starting signature group by bands and number process")

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

                spark_df_rdd = self.read_data(input_batch, self.file_ext)
                logging.info(f"spark_df_rdd has {spark_df_rdd.count()} rows")
                self.total_number_of_documents += spark_df_rdd.count()
                spark_df_rdd = spark_df_rdd.withColumn("number_of_documents", lit(1)).drop(
                    "int_id_column", "document_length"
                )
                logging.info(f"spark_df_rdd has these columns {spark_df_rdd.columns}")
                final_band_df = self.aggregate_dataframes(final_band_df, spark_df_rdd)
                logging.info(f"final_band_df has these columns {final_band_df.columns}")

            logging.info(f"Before filtering, final_band_df has {final_band_df.count()} rows")
            final_band_df = final_band_df.filter(F.col("number_of_documents") > 1)
            logging.info(f"After filtering, final_band_df has {final_band_df.count()} rows")
            final_df = self.aggregate_dataframes(final_df, final_band_df)

        for band_index in range(num_bands):
            bands_path = os.path.join(self.band_output_path, f"band_{band_index}")
            self.input_files, self.file_stats = self.list_files(bands_path, self.file_ext)

            file_batcher = SparkFileBatcher(
                self.input_files,
                self.default_batch_size,
                self.out_data_access,
                os.path.join(self.output_path, "checkpoint.txt"),
            )
            logging.info("Starting signature group by bands and number process")

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

                spark_df_rdd = self.read_data(input_batch, self.file_ext)
                df_filtered = self.filter_by_band_hash(spark_df_rdd, final_df)

                df_grouped = self.group_and_aggregate(df_filtered.drop("document_length"))

                self.write_data(
                    df_grouped,
                    os.path.join(self.clusters_doc_ids_output_path, f"cluster_bands_{band_index}"),
                    self.file_ext,
                )

    def execute(self):
        try:
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
