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
from pyspark.sql.functions import col, collect_list, explode, first, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import udf, when
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


class FDJaccardDistanceCalculator(SparkTransformerRuntime):
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
        self.clusters_output_path = os.path.join(self.output_path, "clusters")
        self.doc2remove_output_path = os.path.join(self.output_path, "doc2remove")
        self.clusters_doc_ids_output_path = os.path.join(self.output_path, "clusters_doc_ids")
        self.doc2remove_output_path = os.path.join(self.output_path, "doc2remove")
        self.cleaned_output_path = os.path.join(self.output_path, "cleaned")
        self.doc2remove_intermediate_output_path = os.path.join(self.output_path, "doc2remove_intermediate")

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

    def run_transform(self):

        # Define the minhash schema
        minhash_schema = StructType(
            [
                StructField("band_hash", LongType(), True),
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
        final_band_df = self.spark.createDataFrame([], minhash_schema)

        logging.info(f"Default_batch_size{self.default_batch_size} rows")

        # Band hash and doc_ids
        self.input_files, self.file_stats = self.list_files(self.clusters_doc_ids_output_path, self.file_ext)
        self.default_batch_size = 1
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
            spark_df_cluster_bands = self.read_data(input_batch, self.file_ext)
            logging.info(f"spark_df_rdd has {spark_df_cluster_bands.count()} rows")

            # Explode spark_df_cluster_bands
            spark_df_cluster_bands_exploded = spark_df_cluster_bands.withColumn(
                "int_id_column", explode("doc_ids")
            ).select("band_hash", "int_id_column")

            # Step 2: read minhash
            self.input_files, self.file_stats = self.list_files(self.minhash_output_path, self.file_ext)
            self.default_batch_size = 1
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
                spark_df_rdd_minhash = self.read_data(input_batch, self.file_ext)

                # Join the exploded DataFrame with the second DataFrame
                joined_df = spark_df_cluster_bands_exploded.join(
                    spark_df_rdd_minhash, on="int_id_column", how="inner"
                ).select("band_hash", "int_id_column", "data")

                final_band_df = final_band_df.union(joined_df)

            # Group by band_hash and aggregate int_id_column, document_length, and minhashes
            result_df = final_band_df.groupby("band_hash").agg(
                F.collect_list(F.struct("int_id_column", "data.document_length", "data.minhashes")).alias(
                    "combined_data"
                )
            )

            # Convert to the desired structure (band_hash, [(int_id_column, document_length, minhashes), ...])
            result = result_df.rdd.map(
                lambda row: (
                    row.band_hash,
                    [(item.int_id_column, item.document_length, item.minhashes) for item in row.combined_data],
                )
            ).collect()

            # Create a new DataFrame from the output data
            output_schema = StructType(
                [
                    StructField("band_hash", LongType(), True),
                    StructField(
                        "combined_data",
                        ArrayType(
                            StructType(
                                [
                                    StructField("int_id_column", LongType(), True),
                                    StructField("document_length", IntegerType(), True),
                                    StructField("minhashes", ArrayType(IntegerType()), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            )

            output_df = self.spark.createDataFrame(result, schema=output_schema)

            # sort data based on document_length
            def sort_combined_data(combined_data):
                return sorted(combined_data, key=lambda x: x.document_length)

            sort_udf = udf(
                sort_combined_data,
                ArrayType(
                    StructType(
                        [
                            StructField("int_id_column", LongType(), True),
                            StructField("document_length", IntegerType(), True),
                            StructField("minhashes", ArrayType(IntegerType()), True),
                        ]
                    )
                ),
            )

            # Apply the UDF to sort the combined_data array
            sorted_df = output_df.withColumn("combined_data", sort_udf(col("combined_data")))

            # Define the cluster processing function
            def cluster_processing(row, threshold: float = 0.8):
                band_hash = row.band_hash
                combined_data = row.combined_data

                candidates_list = {doc_id: mh for doc_id, doc_len, mh in combined_data}

                doc_lengths = {doc_id: doc_len for doc_id, doc_len, mh in combined_data}

                ds = disjoint_set.DisjointSet.from_iterable(
                    candidates_list.keys()
                )  # set with no-overlap (a set = a cluster of similar documents)
                unvisited = set(candidates_list.keys())

                while len(unvisited) > 0:
                    current_doc_id = unvisited.pop()
                    current_mh = candidates_list[current_doc_id]
                    del candidates_list[current_doc_id]
                    for other_doc_id, other_mh in candidates_list.items():
                        distance = Murmur_MH.jaccard(np.array(current_mh), np.array(other_mh))
                        # print(f"distance: {distance} ? {threshold}")
                        d1 = ds.find(current_doc_id)
                        d2 = ds.find(other_doc_id)
                        if d1 != d2:  # union must apply based on canonical node
                            if distance >= threshold:
                                ds.union(d1, d2)
                            # unvisited.discard(other_doc_id) # we should not remove as it can
                            # be used to look for other matching candidates

                local_connected_components = list(ds.itersets(with_canonical_elements=True))

                def get_doc2keep_first(doc_id):
                    return (-doc_lengths[doc_id], -doc_id)

                for _, duplicates_list in local_connected_components:
                    duplicates_list = sorted(duplicates_list, key=get_doc2keep_first)
                    cluster_id = duplicates_list[0]
                    cluster_size = len(duplicates_list)
                    string = ",".join([str(i) for i in duplicates_list])
                    for d in duplicates_list:
                        if d == cluster_id:
                            keep = True
                            # low_threshold = True
                        else:
                            keep = False
                            # distance = Murmur_MH.jaccard(candidates_list[d], candidates_list[cluster_id])
                            # if distance < threshold:
                            #    low_threshold = True
                        # FIXME : DEBUG PURPOSE
                        # low_threshold = False
                        # END FIXME
                        yield (cluster_id, d, doc_lengths[d], cluster_size, keep, string)  # , low_threshold)

                # for item in combined_data:
                #     yield (band_hash, item.int_id_column, item.document_length, item.minhashes)

            # Use flatMap with the custom processing function
            doc_clusters = sorted_df.rdd.flatMap(lambda row: cluster_processing(row))

            doc_cluster_partitions = doc_clusters.getNumPartitions()
            logging.info(f"{doc_clusters.count()} clusters ---- with {doc_cluster_partitions} partitions")

            # Get the schema
            columns = ["cluster_id", self.document_id_column, "doc_len", "cluster_size", "keep", "doc_ids"]
            schema = self.get_schema_fuzzy_dedup(columns)

            # create data frame from document clusters
            doc_in_group_df = self.spark.createDataFrame(doc_clusters, schema=schema)
            logging.info(f"Check doc_in_group_df has {doc_in_group_df.count()} rows")

            fuzzyclustersize_threshold = 1
            out_sdf = doc_in_group_df.filter(F.col("cluster_size") > fuzzyclustersize_threshold).filter(
                F.col("keep") != True
            )  # .drop('low_threshold')
            doc2remove_sdf = out_sdf.select(self.document_id_column, "doc_ids").dropDuplicates()

            # write doc2remove_sdf
            self.write_data(doc2remove_sdf, self.doc2remove_output_path, self.file_ext)
            # remove data
            self.input_files, self.file_stats = self.list_files(self.input_path, self.file_ext)
            file_batcher = SparkFileBatcher(
                self.input_files,
                self.default_batch_size,
                self.out_data_access,
                os.path.join(self.output_path, "checkpoint.txt"),
            )
            logging.info("Starting fuzzy dedupe data clean")

            # Loop through batches until all files processed
            while True:
                # Get next batch
                input_batch, batch_size = file_batcher.next_batch()
                if not input_batch:
                    break

                logging.info(
                    f"Processing batch {file_batcher.current_batch_index} out of {file_batcher.total_batches}"
                )

                # read ids of duplicate data
                sdf_doc2remove = self.read_data(self.doc2remove_output_path, self.file_ext)

                # read raw data
                spark_df = self.read_data(input_batch, self.file_ext)

                matching_cols = [self.document_id_column]

                if sdf_doc2remove is None:
                    final_sdf = spark_df
                else:
                    final_sdf = spark_df.join(sdf_doc2remove, on=matching_cols, how="left_anti")

                # write doc2remove_sdf
                self.write_data(final_sdf, self.cleaned_output_path, self.file_ext)

    def get_schema_fuzzy_dedup(self, columns: list[str]) -> Union[LongType, BooleanType, StringType]:
        """
        Create a StructType schema from a list of column names. Column names
        are used so as to support multiple datatypes e.g., datapile and redpyjama

        Args:
        columns (list of str): List of column names.

        Returns:
        StructType: PySpark StructType schema.
        """

        def get_schema_fuzzy_dedup_column(column_name):
            if column_name in [self.document_id_column, "doc_len", "cluster_size"]:
                return LongType()
            elif column_name == "keep":
                return BooleanType()
            else:
                return StringType()

        fields = [StructField(column, get_schema_fuzzy_dedup_column(column), True) for column in columns]
        schema = StructType(fields)
        return schema

    def execute(self):
        try:
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
