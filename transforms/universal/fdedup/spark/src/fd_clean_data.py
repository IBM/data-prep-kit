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
from pyspark.sql.functions import col, explode, size, udf
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


class FDCleanData(SparkTransformerRuntime):
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
        configs: str,
        step_name: str,
    ):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path
        self.doc2remove_path = os.path.join(self.output_path, "doc2remove")
        self.cleaned_output_path = os.path.join(self.output_path, "cleaned")

        self.file_ext = file_ext
        self.default_batch_size = default_batch_size
        self.document_id_column = document_id_column
        self.contents_column = contents_column
        self.debug = debug
        self.checkpoint_count = checkpoint_count
        self.total_document_size = total_document_size
        self.total_number_of_documents = total_number_of_documents
        self.configs = configs
        self.step_name = step_name
        self.in_out_metadata = {}
        self.execution_name = "execution_" + self.step_name
        self.init_io(self.input_path, self.output_path)
        logging.info("Initialized Spark Fuzzy Dedupe data cleaning")

    def read_docs2remove_list(self):
        self.input_files, self.file_stats = self.list_files(self.doc2remove_path, self.file_ext)
        file_batcher = SparkFileBatcher(
            self.input_files,
            self.default_batch_size,
            self.out_data_access,
            os.path.join(self.output_path, "checkpoint.txt"),
        )
        logging.info("Starting fuzzy dedupe read documents to remove")
        docs_to_remove_list = []
        # Loop through batches until all files processed
        while True:
            # Get next batch
            input_batch, _ = file_batcher.next_batch()
            if not input_batch:
                break

            logging.info(f"Processing batch {file_batcher.current_batch_index} out of {file_batcher.total_batches}")

            # read raw data
            spark_df = self.read_data(input_batch, self.file_ext)

            spark_df_explode = spark_df.withColumn("rm_doc_id", explode(col("doc_ids")))
            docs_to_remove_list.extend([int(row.rm_doc_id) for row in spark_df_explode.select("rm_doc_id").collect()])
        return docs_to_remove_list

    def clean_data(self, docs2remove_list: list):
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
            if self.execution_name not in self.in_out_metadata:
                self.in_out_metadata[self.execution_name] = {
                    self.execution_name + "_status": "started",
                    "default_batch_size": self.default_batch_size,
                    "num_files": file_batcher.num_files,
                    "total_batches": file_batcher.total_batches,
                    "file_index": file_batcher.file_index,
                }
            elif (
                self.execution_name in self.in_out_metadata
                and self.in_out_metadata[self.execution_name][self.execution_name + "_status"] == "error"
            ):
                self.in_out_metadata[self.execution_name][self.execution_name + "_status"] = "progress"
                if int(self.in_out_metadata[self.execution_name]["file_index"]) > 0:
                    file_batcher.file_index = int(self.in_out_metadata[self.execution_name]["file_index"])
                else:
                    file_batcher.file_index = 0
            else:
                self.in_out_metadata[self.execution_name][self.execution_name + "_status"] = "progress"
            # Get next batch
            input_batch, batch_size = file_batcher.next_batch()
            self.in_out_metadata[self.execution_name]["file_index"] = file_batcher.file_index

            if not input_batch:
                break

            logging.info(f"Processing batch {file_batcher.current_batch_index} out of {file_batcher.total_batches}")

            # read raw data
            spark_df = self.read_data(input_batch, self.file_ext)

            # Filter out rows where the 'id' column is in the given list
            final_sdf = spark_df.filter(~spark_df[self.document_id_column].isin(docs2remove_list))

            # write cleaned data
            self.write_data(final_sdf, self.cleaned_output_path, self.file_ext)

    def run_transform(self):
        # read the document ids to remove from doc2remove folder
        docs2remove_list = self.read_docs2remove_list()
        # clean data
        self.clean_data(docs2remove_list)

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
            else:
                logging.info(f"Skipping {self.step_name} because the previous step failed")
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            self._save_metadata(self.execution_name, "error", self.in_out_metadata, self.output_path)
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
