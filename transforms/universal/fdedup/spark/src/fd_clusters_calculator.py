import json
import logging
import os

import numpy as np
from Murmur_MH import Murmur_MH
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    collect_list,
    collect_set,
    desc,
    explode,
    size,
    sum,
    udf,
)
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructField, StructType
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
        debug: bool,
        checkpoint_count: int,
        total_document_size: int,
        total_number_of_documents: int,
        seed: int,
        configs: str,
        step_name: str,
        num_segments: int,
        jaccard_similarity_threshold: int,
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
        self.debug = debug
        self.checkpoint_count = checkpoint_count
        self.total_document_size = total_document_size
        self.total_number_of_documents = total_number_of_documents
        self.seed = seed
        self.configs = configs
        self.step_name = step_name
        self.in_out_metadata = {}
        self.execution_name = "execution_" + self.step_name
        self.init_io(self.input_path, self.output_path)
        self.num_segments = num_segments
        self.jaccard_similarity_threshold = jaccard_similarity_threshold
        self.doc2remove_output_path = os.path.join(self.output_path, "doc2remove")
        logging.info("Initialized Spark Fuzzy Dedupe Clusters Calculator")

    def group_and_aggregate(self, df):
        # # Group by band_hash and aggregate int_id_column into a list and take the first length
        # df_grouped = df.groupBy("band_hash").agg(
        #     collect_list("int_id_column").alias("doc_ids")
        # )
        # Group by band_hash and collect list of int_id_column, then sort the list
        df_grouped = df.groupBy("band_hash").agg(collect_list("int_id_column").alias("doc_ids"))
        return df_grouped

    def run_transform(self):
        num_bands = self.in_out_metadata["num_bands"]
        num_bands_segments = self.num_segments
        num_bands_index = 0
        num_bands_segment_index = 0
        # Define schema
        schema = StructType(
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

        # Define doc2remove schema
        doc2remove_schema = StructType(
            [
                StructField("int_id_column", LongType(), True),
                StructField("doc_ids", ArrayType(LongType()), True),
                StructField("cluster_size", IntegerType(), True),
            ]
        )
        if self.execution_name not in self.in_out_metadata:
            self.in_out_metadata[self.execution_name] = {
                self.execution_name + "_status": "started",
                "default_batch_size": self.default_batch_size,
                "num_files": 0,
                "total_batches": 0,
                "file_index_status": "",
                "file_index": 0,
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
        sements_schema = StructType(
            [
                StructField("band_hash", LongType(), True),
                StructField(
                    "document_data",
                    StructType(
                        [
                            StructField(self.document_id_column, LongType(), True),
                            StructField("minhashes", ArrayType(IntegerType()), True),
                            StructField("document_length", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        docs2remove_list = []
        for band_index in range(num_bands_index, num_bands):
            # track band index
            self.in_out_metadata[self.execution_name]["band_index"] = band_index

            logging.info(f"Building document clusters for band {band_index}")
            for band_segment_index in range(num_bands_segment_index, num_bands_segments):

                # Create an initial empty DataFrame
                band_df = self.spark.createDataFrame([], schema)
                segments_df = self.spark.createDataFrame([], sements_schema)
                bands_path = os.path.join(self.band_output_path, f"band={band_index}/segment={band_segment_index}")
                self.input_files, self.file_stats = self.list_files(bands_path, self.file_ext)
                file_batcher = SparkFileBatcher(
                    self.input_files,
                    self.default_batch_size,
                    self.out_data_access,
                    os.path.join(self.output_path, "checkpoint.txt"),
                )
                while True:
                    if (
                        "file_index_status" in self.in_out_metadata[self.execution_name]
                        and self.in_out_metadata[self.execution_name]["file_index_status"] == "error"
                    ):
                        self.in_out_metadata[self.execution_name]["file_index_status"] = "progress"
                        if int(self.in_out_metadata[self.execution_name]["file_index"]) > 0:
                            file_batcher.file_index = int(self.in_out_metadata[self.execution_name]["file_index"])
                        else:
                            file_batcher.file_index = 0
                    else:
                        self.in_out_metadata[self.execution_name]["file_index_status"] = "progress"

                    self.checkpoint_count += 1
                    # Get next batch
                    input_batch, batch_size = file_batcher.next_batch()
                    # store the index used and other metadata
                    self.in_out_metadata[self.execution_name]["file_index"] = file_batcher.file_index
                    self.in_out_metadata[self.execution_name]["num_files"] = file_batcher.num_files
                    self.in_out_metadata[self.execution_name]["total_batches"] = file_batcher.total_batches
                    # used to track document size
                    self.total_document_size += batch_size
                    if not input_batch:
                        break
                    logging.info(
                        f"Processing batch {file_batcher.current_batch_index} " f"out of {file_batcher.total_batches}"
                    )

                    spark_df = self.read_data(input_batch, self.file_ext)

                    segments_df = segments_df.union(spark_df)
                    # logging.info(f"  Read {spark_df.count()} documents")
                    # self.total_number_of_documents += spark_df.count()
                group_df = segments_df.groupBy("band_hash").agg(collect_set("document_data").alias("doc_ids"))
                group_df = group_df.withColumn("cluster_size", size(col("doc_ids")))
                group_df = group_df.orderBy(desc("cluster_size"))
                cluster_df = group_df.filter(col("cluster_size") > 1)
                # logging.info(f"  Before filtering, {band_df.count()} clusters")
                # logging.info(f"  After filtering, {cluster_df.count()} clusters")
                # Cluster save data
                # self.write_data(
                #     cluster_df,
                #     os.path.join(
                #         self.clusters_doc_ids_output_path, f"cluster_bands={band_index}/segment={band_segment_index}"
                #     ),
                #     self.file_ext,
                # )

                # Code from signature calculator
                sorted_cluster_df = self.sort_clusters(cluster_df)

                docs_to_remove_df, docs_to_remove_list = self.get_docs_to_remove(
                    sorted_cluster_df,
                    doc2remove_schema,
                )
                # logging.info(
                #     f"Band {band_index} Segment {band_segment_index}: docs_to_remove_df has {docs_to_remove_df.count()} rows"
                # )
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

        # Apply the UDF to sort the combined_data array
        sorted_clusters_df = minhash_cluster_df.withColumn("document_data", sort_udf(col("doc_ids")))
        # logging.info(f"sorted_clusters_df has {sorted_clusters_df.count()} rows")
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

    def execute(self):
        try:
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
        except Exception as ex:
            self._save_metadata(self.execution_name, "error", self.in_out_metadata, self.output_path)
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
