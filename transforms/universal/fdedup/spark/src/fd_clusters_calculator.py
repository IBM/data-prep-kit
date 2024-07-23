import json
import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_set, desc, size
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
        num_bands_index = 0
        # Define schema
        schema = StructType(
            [
                StructField("band_hash", LongType(), True),
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

        for band_index in range(num_bands_index, num_bands):
            # track band index
            self.in_out_metadata[self.execution_name]["band_index"] = band_index

            logging.info(f"Building document clusters for band {band_index}")
            # Create an initial empty DataFrame
            band_df = self.spark.createDataFrame([], schema)
            bands_path = os.path.join(self.band_output_path, f"band_{band_index}")
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
                logging.info(f"  Read {spark_df.count()} documents")
                self.total_number_of_documents += spark_df.count()
                group_df = spark_df.groupBy("band_hash").agg(collect_set("int_id_column").alias("doc_ids"))
                group_df = group_df.withColumn("cluster_size", size(col("doc_ids")))
                group_df = group_df.orderBy(desc("cluster_size"))
                band_df = band_df.union(group_df)
            cluster_df = band_df.filter(col("cluster_size") > 1)
            logging.info(f"  Before filtering, {band_df.count()} clusters")
            logging.info(f"  After filtering, {cluster_df.count()} clusters")
            self.write_data(
                cluster_df,
                os.path.join(self.clusters_doc_ids_output_path, f"cluster_bands_{band_index}"),
                self.file_ext,
            )

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
