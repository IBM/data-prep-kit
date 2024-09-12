import logging
import os
from typing import List, Tuple

import numpy as np
import polars as pl
from Murmur_MH import Murmur_MH


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Log format
    handlers=[logging.StreamHandler()],  # Output log messages to the console
)


class FDClustersAndJaccardCalculator:
    def __init__(
        self,
        input_path: str,
        output_path: str,
        file_ext: str,
        document_id_column: str,
        contents_column: str,
        seed: int,
        num_segments: int,
        num_bands: int,
        jaccard_similarity_threshold: int,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.minhash_output_path = os.path.join(self.output_path, "minhashes")
        self.band_output_path = os.path.join(self.output_path, "bands")
        self.clusters_int_id_columns_output_path = os.path.join(self.output_path, "clusters_int_id_columns")
        self.file_ext = file_ext
        self.document_id_column = document_id_column
        self.contents_column = contents_column
        self.seed = seed
        self.num_segments = num_segments
        self.num_bands = num_bands
        self.jaccard_similarity_threshold = jaccard_similarity_threshold
        self.doc2remove_output_path = os.path.join(self.output_path, "doc2remove")
        logging.info("Initialized Spark Fuzzy Dedupe Clusters Calculator")

    def process_bands(self, df: pl.DataFrame) -> pl.DataFrame:
        # Define the schema with specific data types
        schema = {"first_doc": pl.Int64, "docs_to_remove": pl.List(pl.Int64), "docs_to_remove_length": pl.Int64}

        doc_ids_lists = []
        docs_to_remove_lists = []
        len_of_docs2remove_lists = []
        for row in df.iter_rows(named=True):
            doc_ids_list, docs_to_remove_list, len_of_docs2remove_list = self.jaccard_distance_calculation(row)
            doc_ids_lists += doc_ids_list
            docs_to_remove_lists += docs_to_remove_list
            len_of_docs2remove_lists += len_of_docs2remove_list
        processed_rows = pl.DataFrame(
            {
                "first_doc": doc_ids_lists,
                "docs_to_remove": docs_to_remove_lists,
                "docs_to_remove_length": len_of_docs2remove_lists,
            },
            schema=schema,
        )
        return processed_rows

    def jaccard_distance_calculation(self, row: List[pl.Series]) -> list[list]:
        # Process row and return a new list of Series or a new row
        threshold = 0.8
        doc_ids_list = []
        docs_to_remove_list = []
        len_of_docs2remove_list = []
        # Extracting int_id_column values into a list
        doc_list = [item["int_id_column"] for item in row["document_data"]]
        # Creating a dictionary with int_id_column as key and minhashes as value
        doc_minhashes = {item["int_id_column"]: item["minhashes"] for item in row["document_data"]}
        while len(doc_list) > 1:
            docs_to_remove = []
            new_doc_list = []
            # this is the document we are going to keep
            first_doc = doc_list[0]
            first_mh = doc_minhashes[first_doc]
            for int_id_column in doc_list[1:]:
                doc_mh = doc_minhashes[int_id_column]
                distance = Murmur_MH.jaccard(np.array(first_mh), np.array(doc_mh))
                if distance >= threshold:
                    docs_to_remove.append(int_id_column)
                else:
                    new_doc_list.append(int_id_column)
            if len(docs_to_remove) > 0:
                docs_to_remove = list(set(docs_to_remove))
                doc_ids_list.append(first_doc)
                docs_to_remove_list.append(docs_to_remove)
                len_of_docs2remove_list.append(len(docs_to_remove))
            doc_list = new_doc_list

        return doc_ids_list, docs_to_remove_list, len_of_docs2remove_list

    def write_data(self, band_output_path, df, file_name, data_type="parquet"):
        file_path = os.path.join(band_output_path, f"{file_name}.{data_type}")
        if data_type == "parquet":
            df.write_parquet(file_path)
        elif data_type == "csv":
            df.write_csv(file_path)
        elif data_type == "json":
            df.write_json(file_path)
        else:
            raise ValueError(f"Unsupported data_type: {data_type}")

    def run_transform(self):
        # num_bands = self.in_out_metadata["num_bands"]
        num_bands = self.num_bands
        num_bands_segments = self.num_segments
        num_bands_index = 0
        num_bands_segment_index = 0
        schema = pl.Schema({"first_doc": pl.Int64, "docs_to_remove": pl.Int64})
        all_bands_dataframe = pl.DataFrame(schema=schema)
        docs2remove_list = []
        for band_index in range(num_bands_index, num_bands):
            # Define the schema
            schema = pl.Schema(
                {
                    "band_hash": pl.Int64,
                    "document_data": pl.Struct(
                        {"int_id_column": pl.Int64, "minhashes": pl.List(pl.Int32), "document_length": pl.Int32}
                    ),
                }
            )
            bands_dataframe = pl.DataFrame(schema=schema)
            logging.info(f"Building document clusters for band {band_index}")
            for band_segment_index in range(num_bands_segment_index, num_bands_segments):
                # create path
                bands_path = os.path.join(self.band_output_path, f"band={band_index}/segment={band_segment_index}")

                # read all files in a given segment
                polars_df = pl.read_parquet(bands_path + "/*.parquet")
                bands_dataframe = bands_dataframe.vstack(polars_df)
            # clustering
            bands_dataframe_groups = bands_dataframe.group_by("band_hash").agg("document_data")
            bands_dataframe_cluster = bands_dataframe_groups.with_columns(
                cluster_length=pl.col("document_data").list.len()
            ).filter(pl.col("cluster_length") > 1)

            bands_dataframe_response = self.process_bands(bands_dataframe_cluster)

            filtered_doc2remove_dataframe = bands_dataframe_response.filter(pl.col("docs_to_remove_length") > 0)
            filtered_doc2remove_dataframe = filtered_doc2remove_dataframe.drop("docs_to_remove_length")

            # Explode the 'minhashes' column
            doc2remove_exploded_dataframe = filtered_doc2remove_dataframe.explode("docs_to_remove")

            # Convert the 'exploded_minhashes' column to a list of integers
            docs_to_remove_list = doc2remove_exploded_dataframe.select("docs_to_remove").to_series().to_list()

            docs2remove_list.extend(docs_to_remove_list)
            docs2remove_list = list(set(docs2remove_list))

            logging.info(
                f"After band {band_index} Segment {band_segment_index}, {len(docs2remove_list)} documents marked for removal"
            )
            # write doc2remove_sdf
            self.write_data(
                self.doc2remove_output_path,
                doc2remove_exploded_dataframe,
                f"doc_remove_band_{band_index}_segment_{band_segment_index}.parquet",
                self.file_ext,
            )

    def execute(self):
        try:
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")


# var
output_path = "/Users/nelson/workspace/Research/DataPreprocessing/ibm/active/data-prep-kit/transforms/universal/fdedup/python/output/"
input_path = "/Users/nelson/workspace/Research/DataPreprocessing/ibm/active/data-prep-kit/transforms/universal/fdedup/python/output/"
file_ext = "parquet"
document_id_column = "int_id_column"
contents_column = "contents"
seed = 42
num_segments = 1
num_bands = 5
jaccard_similarity_threshold = 0.8

# Instantiate the class
my_instance = FDClustersAndJaccardCalculator(
    input_path,
    output_path,
    file_ext,
    document_id_column,
    contents_column,
    seed,
    num_segments,
    num_bands,
    jaccard_similarity_threshold,
)

# Call the execute method
my_instance.execute()
