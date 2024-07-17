import logging
import os

from pyspark.sql.functions import col, explode
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
            # Get next batch
            input_batch, batch_size = file_batcher.next_batch()
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
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")
        finally:
            self.stop()
            logging.info("Stopped the spark session for generating doc signatures")
