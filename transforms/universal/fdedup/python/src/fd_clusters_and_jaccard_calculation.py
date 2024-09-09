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

    def sort_clusters(self, minhash_cluster_df: pl.DataFrame) -> pl.DataFrame:
        sorted_df = minhash_cluster_df.sort(by=["cluster_length"], descending=True)
        return sorted_df

    def process_bands(self, df: pl.DataFrame) -> pl.DataFrame:
        # Apply row processing
        processed_rows = df.map_rows(lambda row: self.jaccard_distance_calculation(row))
        return processed_rows

    def jaccard_distance_calculation(self, row: List[pl.Series]) -> List[pl.Series]:
        # Process row and return a new list of Series or a new row
        threshold = 0.8
        docs_to_remove = []

        # Extracting int_id_column values into a list
        doc_list = [item["int_id_column"] for item in row[1]]

        # Creating a dictionary with int_id_column as key and minhashes as value
        doc_minhashes = {item["int_id_column"]: item["minhashes"] for item in row[1]}

        # this is the document we are going to keep
        first_doc = doc_list[0]
        first_mh = doc_minhashes[first_doc]

        for int_id_column in doc_list[1:]:
            doc_mh = doc_minhashes[int_id_column]
            distance = Murmur_MH.jaccard(np.array(first_mh), np.array(doc_mh))
            if distance >= threshold:
                docs_to_remove.append(int_id_column)
        docs_to_remove = list(set(docs_to_remove))

        return (first_doc, docs_to_remove, len(docs_to_remove))

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

            bands_dataframe_sorted = self.sort_clusters(bands_dataframe_cluster)

            bands_dataframe_response = self.process_bands(bands_dataframe_sorted)

            # Rename columns to match the new schema
            bands_dataframe_response = bands_dataframe_response.rename(
                {"column_0": "first_doc", "column_1": "docs_to_remove", "column_2": "docs_to_remove_length"}
            )

            # Apply the schema (note: Polars does not have a direct method to enforce schema;
            # this is done by aligning column names and types)
            doc2remove_dataframe = bands_dataframe_response.select(
                [
                    pl.col("first_doc").cast(pl.Int64),
                    pl.col("docs_to_remove").cast(pl.List(pl.Int64)),
                    pl.col("docs_to_remove_length").cast(pl.Int64),
                ]
            )

            filtered_doc2remove_dataframe = doc2remove_dataframe.filter(pl.col("docs_to_remove_length") > 0)

            # Explode the 'minhashes' column
            doc2remove_exploded_dataframe = filtered_doc2remove_dataframe.explode("docs_to_remove")

            # Add a new column with the same values as 'docs_to_remove' list
            final_df = doc2remove_exploded_dataframe.with_columns(pl.col("docs_to_remove").alias("rm_int_id_column"))

            # Convert the 'exploded_minhashes' column to a list of integers
            docs_to_remove_list = final_df.select("rm_int_id_column").to_series().to_list()

            docs2remove_list.extend(docs_to_remove_list)
            docs2remove_list = list(set(docs2remove_list))

            logging.info(
                f"After band {band_index} Segment {band_segment_index}, {len(docs2remove_list)} documents marked for removal"
            )
            # write doc2remove_sdf
            self.write_data(
                self.doc2remove_output_path,
                filtered_doc2remove_dataframe,
                f"doc_remove_band_{band_index}_segment_{band_segment_index}.parquet",
                self.file_ext,
            )

    def execute(self):
        try:
            self.run_transform()
            logging.info("Finished grouping by band hash and band number")
        except Exception as ex:
            logging.error(f"Failed to group by band hash and band number: {ex}")


# update the path accordingly
output_path = "/data-prep-kit/transforms/universal/fdedup/python/output/"
input_path = "/data-prep-kit/transforms/universal/fdedup/python/output/"
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
