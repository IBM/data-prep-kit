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
    ):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path
        self.minhash_output_path = os.path.join(self.output_path, "minhashes")
        self.band_output_path = os.path.join(self.output_path, "bands")
        self.clusters_doc_ids_output_path = os.path.join(self.output_path, "clusters_doc_ids")
        self.file_ext = file_ext
        self.default_batch_size = 1000000
        self.document_id_column = document_id_column
        self.contents_column = contents_column
        self.debug = debug
        self.checkpoint_count = checkpoint_count
        self.total_document_size = total_document_size
        self.total_number_of_documents = total_number_of_documents
        self.seed = seed
        self.configs = configs
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
        # Read number of bands from metadata
        spark_df_metadata = self.read_data(os.path.join(self.band_output_path, "metadata"), "json")
        in_out_metadata_val = spark_df_metadata.toJSON().collect()
        json_object = json.loads(in_out_metadata_val[0])
        num_bands = json.loads(json_object.get("value"))["num_bands"]
        # Define schema
        schema = StructType(
            [
                StructField("band_hash", LongType(), True),
                StructField("doc_ids", ArrayType(LongType()), True),
                StructField("cluster_size", IntegerType(), True),
            ]
        )

        for band_index in range(num_bands):
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
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
