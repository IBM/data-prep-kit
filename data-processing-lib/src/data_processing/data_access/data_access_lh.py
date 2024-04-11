import json
from typing import Any

import pyarrow
from data_processing.data_access import DataAccess, DataAccessS3
from lakehouse import (
    CosCredentials,
    Datasource,
    JobDetails,
    JobStats,
    LakehouseForProcessingTask,
    SourceCodeDetails,
)
from data_processing.utils import get_logger


logger = get_logger(__name__)


class DataAccessLakeHouse(DataAccess):
    """
    Implementation of the Base Data access class for lakehouse-based data access
    """

    def __init__(
            self,
            s3_credentials: dict[str, str],
            lakehouse_config: dict[str, str] = None,
            d_sets: list[str] = None,
            checkpoint: bool = False,
            m_files: int = -1,
            n_samples: int = 1,
            files_to_use: list[str] = ['.parquet'],
    ):
        """
        Create data access class for lake house based configuration
        :param s3_credentials: dictionary of cos credentials
        :param lakehouse_config: dictionary of lakehouse info
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return
        :param n_samples: amount of files to randomly sample
        :param files_to_use: files extensions of files to include
        """
        if lakehouse_config is None:
            self.output_folder = None
        else:
            cos_cred = CosCredentials(
                key=s3_credentials["access_key"],
                secret=s3_credentials["secret_key"],
                region="us-east",
                endpoint=s3_credentials["url"],
            )
            self.lh = LakehouseForProcessingTask(
                input_table_name=lakehouse_config["input_table"],
                dataset=lakehouse_config["input_dataset"],
                version=lakehouse_config["input_version"],
                output_table_name=lakehouse_config["output_table"],
                output_path=lakehouse_config["output_path"],
                token=lakehouse_config["token"],
                environment=lakehouse_config["lh_environment"],
                cos_credentials=cos_cred,
            )
            self.output_folder = self.lh.get_output_data_path()
        self.S3 = DataAccessS3(
            s3_credentials=s3_credentials,
            s3_config={
                "input_folder": self.lh.get_input_data_path(),
                "output_folder": self.output_folder,
            },
            d_sets=d_sets,
            checkpoint=checkpoint,
            m_files=m_files,
            n_samples=n_samples,
            files_to_use=files_to_use
        )
        self.n_samples = n_samples

    def get_num_samples(self) -> int:
        """
        Get number of samples for input
        :return: Number of samples
        """
        return self.n_samples

    def get_output_folder(self) -> str:
        """
        Get output folder as a string
        :return: output_folder
        """
        return self.output_folder

    def get_files_to_process_internal(self) -> tuple[list[str], dict[str, float]]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        """
        return self.S3.get_files_to_process()

    def get_table(self, path: str) -> pyarrow.table:
        """
        Get pyArrow table for a given path
        :param path - file path
        :return: pyArrow table or None, if the table read failed
        """
        return self.S3.get_table(path=path)

    def get_output_location(self, path: str) -> str:
        """
        Get output location based on input
        :param path: input file location
        :return: output file location
        """
        if self.output_folder is None:
            logger.error("Getting output location. Lake house is not configured, returning None")
            return None
        return self.lh.get_output_file_path_from(file_path=path)

    @staticmethod
    def _add_table_metadata(table: pyarrow.Table) -> pyarrow.Table:
        """
        Save table to a given location fixing schema, required for lakehouse
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        # update schema to ensure part ids to be there
        fields = []
        columns = table.column_names
        tbl_metadata = table.schema.metadata
        if tbl_metadata is None:
            tbl_metadata = {}
        for index in range(len(table.column_names)):
            field = table.field(index)
            fields.append(field.with_metadata({"PARQUET:field_id": f"{index + 1}"}))
            tbl_metadata[columns[index]] = json.dumps({"PARQUET:field_id": f"{index + 1}"}).encode()
        schema = pyarrow.schema(fields, metadata=tbl_metadata)
        return pyarrow.Table.from_arrays(arrays=list(table.itercolumns()), schema=schema)

    def save_table(self, path: str, table: pyarrow.Table) -> tuple[int, dict[str, Any]]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        if self.output_folder is None:
            logger.error("Saving table. Lake house is not configured, operation skipped")
            return 0, None
        table_with_metadata = self._add_table_metadata(table=table)
        l, repl = self.S3.save_table(path=path, table=table_with_metadata)
        if repl is None:
            return l, {}

        # check if table exists and create output table using schema from pyarrow table
        self.lh.check_and_create_output_table_from_pyarrow(table)
        # update output Iceberg table
        self.lh.update_table(path)
        return l, repl

    def save_job_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        """
        Save metadata
        :param metadata: a dictionary, containing the following keys
        (see https://github.ibm.com/arc/dmf-library/issues/158):
            "pipeline",
            "job details",
            "code",
            "job_input_params",
            "execution_stats",
            "job_output_stats"
        two additional elements:
            "source"
            "target"
        are filled bu implementation
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        if self.output_folder is None:
            logger.error("Lake house is not configured, save job metadata operation skipped")
            return None
        metadata["source"] = {
            "name": self.lh.input_table_name,
            "type": "table",
            "snapshot_id": str(self.lh.get_input_table_metadata().snapshot_id),
            "dataset": self.lh.dataset,
            "version": self.lh.version,
            "path": self.S3.input_folder,
        }
        metadata["target"] = {
            "name": self.lh.output_table_name,
            "type": "table",
            "snapshot_id": str(self.lh.get_output_table_metadata().snapshot_id),
            "dataset": self.lh.dataset,
            "version": self.lh.version,
            "path": self.lh.output_path,
        }
        l, repl = self.S3.save_file(
            path=f"{self.S3.output_folder}/metadata.json", data=json.dumps(metadata, indent=2).encode()
        )
        if repl is None:
            return repl
        # Save metadata to LH
        stats = JobStats(
            pipeline_id=metadata["pipeline"],
            job_details=JobDetails(
                id=metadata["job details"]["job id"],
                name=metadata["job details"]["job name"],
                type=metadata["job details"]["job type"],
                category=metadata["job details"]["job category"],
                status=metadata["job details"]["status"],
                started_at=metadata["job details"]["start_time"],
                completed_at=metadata["job details"]["end_time"],
            ),
            source_code_details=SourceCodeDetails(
                url=metadata["code"]["github"],
                commit_hash=metadata["code"]["commit_hash"],
                path=metadata["code"]["path"],
            ),
            sources=[
                Datasource(
                    name=self.lh.input_table_name,
                    type="table",
                    snapshot_id=metadata["source"]["snapshot_id"],
                    dataset=self.lh.dataset,
                    version=self.lh.version,
                    path=metadata["source"]["path"],
                )
            ],
            targets=[
                Datasource(
                    name=self.lh.output_table_name,
                    type="table",
                    snapshot_id=metadata["target"]["snapshot_id"],
                    dataset=self.lh.dataset,
                    version=self.lh.version,
                    path=metadata["target"]["path"],
                )
            ],
            job_input_params=metadata["job_input_params"],
            execution_stats=metadata["execution_stats"],
            job_output_stats=metadata["job_output_stats"],
        )
        self.lh.save_stats(stats)

    def get_file(self, path: str) -> bytes:
        """
        Get file as a byte array
        :param path: file path
        :return: bytes array of file content
        """
        return self.S3.get_file(path)

    def save_file(self, path: str, data: bytes) -> dict[str, Any]:
        """
        Save byte array to the file
        :param path: file path
        :param data: byte array
        :return: a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        return self.S3.save_file(path=path, data=data)

    def get_folder_files(self, path: str, extensions: list[str] = None, return_data: bool = True) -> dict[str, bytes]:
        """
        Get a list of byte content of files. The path here is an absolute path and can be anywhere.
        The current limitation for S3 and Lakehouse is that it has to be in the same bucket
        :param path: file path
        :param extensions: a list of file extensions to include. If None, then all files from this and
                           child ones will be returned
        :param return_data: flag specifying whether the actual content of files is returned (True), or just
                            directory is returned (False)
        :return: A dictionary of file names/binary content will be returned
        """
        return self.S3.get_folder_files(path=path, extensions=extensions, return_data=return_data)
