from data_access_s3 import *
from lakehouse import (
    CosCredentials,
    Datasource,
    JobDetails,
    JobStats,
    LakehouseForProcessingTask,
    SourceCodeDetails,
)


class DataAccessLakeHouse(DataAccess):
    """
    Implementation of the Base Data access class for lakehouse-based data access
    """

    def __init__(
        self,
        s3_credentials: dict[str, str],
        lakehouse_config: dict[str, str],
        d_sets: list[str],
        checkpoint: bool,
        m_files: int,
    ):
        """
        Create data access class for lake house based configuration
        :param s3_credentials: dictionary of cos credentials
        :param lakehouse_config: dictionary of lakehouse info
        :param d_sets list of the data sets to use
        :param checkpoint: flag to return only files that do not exist in the output directory
        :param m_files: max amount of files to return"""
        cos_cred = CosCredentials(
            key=s3_credentials["access_key"],
            secret=s3_credentials["secret_key"],
            region="us-east",
            endpoint=s3_credentials["cos_url"],
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
        self.S3 = DataAccessS3(
            s3_credentials=s3_credentials,
            s3_config={
                "input_folder": self.lh.get_input_table_path(),
                "output_folder": lakehouse_config["output_path"],
            },
            d_sets=d_sets,
            checkpoint=checkpoint,
            m_files=m_files,
        )

    def get_files_to_process(self) -> tuple[list[str], dict[str, float]]:
        """
        Get files to process
        :return: list of files and a dictionary of the files profile:
            "max_file_size_MB",
            "min_file_size_MB",
            "avg_file_size_MB",
            "total_file_size_MB"
        """
        return self.S3.get_files_to_process()

    def get_table(self, path: str) -> pa.table:
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
        return self.lh.get_output_file_path_from(file_path=path)

    #    def get_input_missing_from_output(self) -> list[str]:
    #        """
    #        Compute the file difference between input and output folders, as in a set operation: A - B,
    #        that is: files that are in input, that are not in output, ignoring any extra files in output
    #        """
    #        diff = self.lh.diff_parquet_input_output()
    #        input_path = self.lh.get_input_table_path()
    #        diff = list(map(lambda x: input_path + x, diff))
    #        return diff

    def save_table(self, path: str, table: pa.Table) -> tuple[int, dict[str, Any]]:
        """
        Save table to a given location
        :param path: location to save table
        :param table: table
        :return: size of table in memory and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """

        l, repl = self.S3.save_table_with_schema(path=path, table=table)
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
            path=f"{self.S3.output_folder}metadata.json", data=json.dumps(metadata, indent=2).encode()
        )
        if repl is None:
            return l, repl
        # Save metadata to LH
        stats = JobStats(
            pipeline_id=metadata["pipeline"],
            job_details=JobDetails(
                id=metadata["job details"]["job id"],
                name=metadata["job details"]["job name"],
                type=metadata["job details"]["job_type"],
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

    def get_folder_files(self, path: str, extensions: list[str]) -> list[bytes]:
        """
        Get a list of byte content of files
        :param path: file path
        :param extensions: a list of file extensions to include
        :return:
        """
        return self.S3.get_folder_files(path=path, extensions=extensions)
