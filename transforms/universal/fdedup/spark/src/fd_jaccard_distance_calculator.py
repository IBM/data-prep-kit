import json
import logging
import os

import numpy as np
from Murmur_MH import Murmur_MH
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, size, sum, udf
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
        step_name: str,
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
        self.salting_size = salting_size
        self.debug = debug
        self.language = language
        self.checkpoint_count = checkpoint_count
        self.total_document_size = total_document_size
        self.total_number_of_documents = total_number_of_documents
        self.seed = seed
        self.configs = configs
        self.step_name = step_name
        self.in_out_metadata = {}
        self.execution_name = "execution_" + self.step_name
        self.init_io(self.input_path, self.output_path)
        server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
        if server_port_https == -1:
            # if the code is running locally, add Murmur_MH.py to the py files used by the Spark context
            murmur_mh_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Murmur_MH.py")
            self.spark.sparkContext.addPyFile(murmur_mh_filepath)
        logging.info("Initialized Spark Fuzzy Dedupe document to remove calculation")

    def get_num_bands_from_metadata(self) -> int:
        # Read number of bands from metadata
        spark_df_metadata = self.read_data(os.path.join(self.output_path, "metadata"), "json")
        in_out_metadata_val = spark_df_metadata.toJSON().collect()
        json_object = json.loads(in_out_metadata_val[0])
        num_bands = json.loads(json_object.get("value"))["num_bands"]
        return num_bands

    def define_schemas(self) -> tuple[StructType, StructType, StructType]:
        # Define cluster schema
        # cluster_schema = StructType(
        #     [
        #         StructField("band_hash", LongType(), True),
        #         StructField("doc_ids", ArrayType(LongType()), True),
        #         StructField("cluster_size", IntegerType(), True),
        #     ]
        # )

        # Define schema
        cluster_schema = StructType(
            [
                StructField("band_hash", LongType(), True),
                StructField(
                    "document_data",
                    ArrayType(
                        StructType(
                            [
                                StructField(self.document_id_column, LongType(), True),
                                StructField("minhashes", ArrayType(IntegerType()), True),
                                StructField("document_length", IntegerType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField("cluster_size", IntegerType(), True),
            ]
        )

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
        # Define doc2remove schema
        doc2remove_schema = StructType(
            [
                StructField("int_id_column", LongType(), True),
                StructField("doc_ids", ArrayType(LongType()), True),
                StructField("cluster_size", IntegerType(), True),
            ]
        )
        return cluster_schema, minhash_schema, doc2remove_schema

    def load_band_clusters(self, cluster_schema: StructType, band_index: int, band_segment_index: int) -> DataFrame:
        cluster_band_df = self.spark.createDataFrame([], cluster_schema)
        logging.info(f"Loading clusters for band {band_index}")
        # Band hash and doc_ids
        band_cluster_doc_ids_path = os.path.join(
            self.clusters_doc_ids_output_path, f"cluster_bands={band_index}/segment={band_segment_index}"
        )

        self.input_files, self.file_stats = self.list_files(
            band_cluster_doc_ids_path,
            self.file_ext,
        )
        batch_size = 10000000
        file_batcher = SparkFileBatcher(
            self.input_files,
            batch_size,
            self.out_data_access,
            os.path.join(self.output_path, "checkpoint.txt"),
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
            spark_df_cluster_bands = self.read_data(input_batch, self.file_ext)
            logging.info(f"spark_df_cluster_bands has {spark_df_cluster_bands.count()} rows")
            cluster_band_df = cluster_band_df.union(spark_df_cluster_bands)
        logging.info(f"Band {band_index} has {cluster_band_df.count()} clusters")
        return cluster_band_df

    def purge_clusters(self, band_clusters: DataFrame, docs_to_remove: set[LongType]) -> DataFrame:
        def remove_doc_ids(doc_list, docs_to_remove):
            return [x for x in doc_list if x not in docs_to_remove]

        remove_integers_udf = udf(lambda doc_list: remove_doc_ids(doc_list, docs_to_remove), ArrayType(LongType()))
        df_with_removed_docs = band_clusters.withColumn(
            "doc_ids_filtered", remove_integers_udf(band_clusters["doc_ids"])
        )
        df_with_removed_docs = df_with_removed_docs.withColumn("purged_cluster_size", size(col("doc_ids_filtered")))
        purged_cluster_df = df_with_removed_docs.filter(col("purged_cluster_size") > 0)
        purged_cluster_df = purged_cluster_df.drop(*["doc_ids_filtered", "purged_cluster_size"])
        return purged_cluster_df

    def add_minhash_to_clusters(self, cluster_band_df: DataFrame, minhash_schema: StructType) -> DataFrame:
        # Explode spark_df_cluster_bands
        spark_df_cluster_bands_exploded = cluster_band_df.withColumn("int_id_column", explode("doc_ids")).select(
            "band_hash", "int_id_column"
        )
        logging.info(f"spark_df_cluster_bands_exploded has {spark_df_cluster_bands_exploded.count()} rows")
        # read minhash
        minhash_band_df = self.spark.createDataFrame([], minhash_schema)
        self.input_files, self.file_stats = self.list_files(self.minhash_output_path, self.file_ext)
        file_batcher = SparkFileBatcher(
            self.input_files,
            self.default_batch_size,
            self.out_data_access,
            os.path.join(self.output_path, "checkpoint.txt"),
        )
        logging.info("Adding minhashes to the cluster documents")

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

            minhash_band_df = minhash_band_df.union(joined_df)

        logging.info(f"minhash_band_df has {minhash_band_df.count()} rows")
        # Group by band_hash and aggregate int_id_column, document_length, and minhashes
        minhash_cluster_df = minhash_band_df.groupby("band_hash").agg(
            F.collect_list(F.struct("int_id_column", "data.document_length", "data.minhashes")).alias("combined_data")
        )
        logging.info(f"minhash_cluster_df has {minhash_cluster_df.count()} rows")
        # first_row = minhash_cluster_df.take(1)[0]
        # print("minhash_cluster_df")
        # print(first_row)
        return minhash_cluster_df

    def sort_clusters(self, minhash_cluster_df: DataFrame) -> DataFrame:
        def sort_combined_data(combined_data):
            return sorted(combined_data, key=lambda x: (-x.document_length, x.int_id_column))

        sort_udf = udf(
            sort_combined_data,
            ArrayType(
                StructType(
                    [
                        StructField("int_id_column", LongType(), True),
                        StructField("minhashes", ArrayType(IntegerType()), True),
                        StructField("document_length", IntegerType(), True),
                    ]
                )
            ),
        )
        #
        # # Define schema
        # schema = StructType(
        #     [
        #         StructField("band_hash", LongType(), True),
        #         StructField(
        #             "document_data",
        #             ArrayType(StructType(
        #                 [
        #                     StructField(self.document_id_column, LongType(), True),
        #                     StructField("minhashes", ArrayType(IntegerType()), True),
        #                     StructField("document_length", IntegerType(), True),
        #
        #                 ]
        #             )),
        #             True,
        #         ),
        #         StructField("cluster_size", IntegerType(), True),
        #     ]
        # )
        #
        # Apply the UDF to sort the combined_data array
        sorted_clusters_df = minhash_cluster_df.withColumn("document_data", sort_udf(col("document_data")))
        logging.info(f"sorted_clusters_df has {sorted_clusters_df.count()} rows")
        # first_row = sorted_clusters_df.take(1)[0]
        # print("sorted_clusters_df")
        # print(first_row)
        return sorted_clusters_df

    def get_docs_to_remove(
        self,
        sorted_cluster_df: DataFrame,
        doc2remove_schema: StructType,
    ) -> tuple[DataFrame, list]:
        # Define the cluster processing function
        def cluster_processing(row, threshold: float = self.jaccard_similarity_threshold):
            docs_to_remove = []
            combined_data = row.document_data
            print(combined_data)
            doc_list = [doc_id for doc_id, _, _ in combined_data]
            doc_minhashes = {doc_id: mh for doc_id, mh, _ in combined_data}
            # this is the document we are going to keep
            first_doc = doc_list[0]
            first_mh = doc_minhashes[first_doc]
            for doc_id in doc_list[1:]:
                doc_mh = doc_minhashes[doc_id]
                distance = Murmur_MH.jaccard(np.array(first_mh), np.array(doc_mh))
                if distance >= threshold:
                    docs_to_remove.append(doc_id)
            docs_to_remove = list(set(docs_to_remove))
            yield (first_doc, docs_to_remove, len(docs_to_remove))

        # Use flatMap with the custom processing function
        doc_clusters = sorted_cluster_df.rdd.flatMap(lambda row: cluster_processing(row))
        docs_to_remove_df = self.spark.createDataFrame(doc_clusters, doc2remove_schema)
        docs_to_remove_df = docs_to_remove_df.filter(col("cluster_size") > 0)
        docs_to_remove_df = docs_to_remove_df.drop("cluster_size")
        docs_to_remove_explode = docs_to_remove_df.withColumn("rm_doc_id", explode(col("doc_ids")))
        docs_to_remove_list = [int(row.rm_doc_id) for row in docs_to_remove_explode.select("rm_doc_id").collect()]
        return docs_to_remove_df, docs_to_remove_list

    def run_transform(self):
        num_bands = self.in_out_metadata["num_bands"]
        num_bands_segments = self.in_out_metadata["num_minhash_bands"]
        num_bands_index = 0
        num_bands_segment_index = 0

        # num_bands = self.get_num_bands_from_metadata()
        cluster_schema, minhash_schema, doc2remove_schema = self.define_schemas()
        docs2remove_list = []

        if self.execution_name not in self.in_out_metadata:
            self.in_out_metadata[self.execution_name] = {
                self.execution_name + "_status": "started",
                "default_batch_size": self.default_batch_size,
                "band_index": 0,
            }
        elif (
            self.execution_name in self.in_out_metadata
            and self.in_out_metadata[self.execution_name][self.execution_name + "_status"] == "error"
        ):
            self.in_out_metadata[self.execution_name][self.execution_name + "_status"] = "progress"
            if int(self.in_out_metadata[self.execution_name]["band_index"]) > 0:
                num_bands_index = int(self.in_out_metadata[self.execution_name]["band_index"])
            else:
                num_bands_index = 0
        else:
            self.in_out_metadata[self.execution_name][self.execution_name + "_status"] = "progress"

        for band_index in range(num_bands_index, num_bands):

            for band_segment_index in range(num_bands_segment_index, num_bands_segments):
                # load the clusters calculated for the band (through band hash groupBy)
                # we cannot slice the band clusters so the only thing we can do is
                # to setup a batch size so that the data loaded during each batch is
                # small enough that it does not overflow
                cluster_band_df = self.load_band_clusters(cluster_schema, band_index, band_segment_index)
                # exclude the clusters for which all the docs were already removed
                # if docs2remove_list:
                #     purged_cluster_df = self.purge_clusters(cluster_band_df, set(docs2remove_list))
                # else:
                #     purged_cluster_df = cluster_band_df

                purged_cluster_df = cluster_band_df
                # num_clusters = purged_cluster_df.count()
                # logging.info(f"Before re-checking Jaccard distance, band {band_index} has {num_clusters} clusters")
                # num_docs_to_remove = purged_cluster_df.select(sum(col("cluster_size"))).collect()[0][0]
                # # we will keep one document from each cluster, so need to adjust num_docs_to_remove
                # num_docs_to_remove -= num_clusters
                # logging.info(
                #     f"Before re-checking Jaccard distance, band {band_index} has {num_docs_to_remove} documents to remove"
                # )
                # Identify the list of documents to remove:
                # Sort the documents inside each cluster by size, and double-check
                # that Jaccard similarity between docs in the same cluster is above
                # a given threshold
                # minhash_cluster_df = self.add_minhash_to_clusters(
                #     purged_cluster_df,
                #     minhash_schema,
                # )
                sorted_cluster_df = self.sort_clusters(purged_cluster_df)

                docs_to_remove_df, docs_to_remove_list = self.get_docs_to_remove(
                    sorted_cluster_df,
                    doc2remove_schema,
                )
                logging.info(
                    f"Band {band_index} Segment {band_segment_index}: docs_to_remove_df has {docs_to_remove_df.count()} rows"
                )
                logging.info(
                    f"Band {band_index} Segment {band_segment_index}: docs_to_remove_list has {len(docs_to_remove_list)} elements"
                )

                docs2remove_list.extend(docs_to_remove_list)
                docs2remove_list = list(set(docs2remove_list))
                logging.info(
                    f"After band {band_index} Segment {band_segment_index}, {len(docs2remove_list)} documents marked for removal"
                )
                # write doc2remove_sdf
                self.write_data(docs_to_remove_df, self.doc2remove_output_path, self.file_ext)

    def execute(self):
        # try:
        # load metadata
        self.in_out_metadata = self._load_metadata(self.output_path)
        prefix, step_number, prev_step_name = self.extract_step_info(self.execution_name)
        if (
            self.execution_name in self.in_out_metadata
            and self.execution_name + "_status" in self.in_out_metadata[self.execution_name]
            and self.in_out_metadata[self.execution_name][self.execution_name + "_status"] == "complete"
        ):
            logging.info(f"Skipping {self.step_name} because its complete")
        elif self.in_out_metadata[prev_step_name][prev_step_name + "_status"] == "complete":
            self.run_transform()
            self._save_metadata(self.execution_name, "complete", self.in_out_metadata, self.output_path)
            logging.info("Finished grouping by band hash and band number")
        else:
            logging.info(f"Skipping {self.step_name} because the previous step failed")
        # except Exception as ex:
        #     self._save_metadata(self.execution_name, "error", self.in_out_metadata, self.output_path)
        #     logging.error(f"Failed to group by band hash and band number: {ex}")
        # finally:
        self.stop()
        # logging.info("Stopped the spark session for generating doc signatures")
