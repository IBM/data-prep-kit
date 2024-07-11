import json
import logging
import os
import socket
from typing import Union

import yaml
from data_access import S3, Local
from pyspark.sql import DataFrame, SparkSession


class SparkTransformerRuntime:
    def __init__(self):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.INFO,
        )
        server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
        if server_port_https == -1:
            # we are running locally, use the spark_profile_local.yaml file for configuration
            config_filepath = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "config", "spark_profile_local.yaml")
            )
            with open(config_filepath, "r") as config_fp:
                spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
            app_name = spark_config.get("spark.app.name", "my-spark-app")
            self.spark = SparkSession.builder.appName(app_name).config(map=spark_config).getOrCreate()
        else:
            # we are running in Kubernetes, use spark_profile.yaml and
            # environment variables for configuration

            server_port = os.environ["KUBERNETES_SERVICE_PORT"]
            master_url = f"k8s://https://kubernetes.default:{server_port}"

            # Read Spark configuration profile
            config_filepath = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "config", "spark_profile.yaml")
            )
            with open(config_filepath, "r") as config_fp:
                spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
            spark_config["spark.submit.deployMode"] = "client"

            # configure the executor pods from template
            executor_pod_template_file = os.path.join(
                os.path.dirname(__file__),
                "templates",
                "spark-executor-pod-template.yaml",
            )
            spark_config["spark.kubernetes.executor.podTemplateFile"] = executor_pod_template_file
            spark_config["spark.kubernetes.container.image.pullPolicy"] = "Always"

            # Pass the driver IP address to the workers for callback
            myservice_url = socket.gethostbyname(socket.gethostname())
            spark_config["spark.driver.host"] = myservice_url
            spark_config["spark.driver.bindAddress"] = "0.0.0.0"

            spark_config["spark.decommission.enabled"] = True

            spark_config["spark.jars.ivy"] = "/opt/spark/work-dir/.ivy2"

            logging.info(f"Launching Spark Session with configuration\n" f"{yaml.dump(spark_config, indent=2)}")
            app_name = spark_config.get("spark.app.name", "my-spark-app")
            self.spark = (
                SparkSession.builder.master(master_url).appName(app_name).config(map=spark_config).getOrCreate()
            )

        self.spark.conf.set("spark.sql.crossJoin.enabled", "true")
        self.spark.conf.set("*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink")
        self.spark.conf.set("*.sink.csv.period", "1")
        self.spark.conf.set("*.sink.csv.unit", "seconds")
        self.spark.conf.set("*.sink.csv.directory", "/opt/spark/work-dir/")

        self.file_index = 0

        print("=============================================")
        print(self.spark._sc._conf.get("spark.driver.memory"))
        executors = self.spark._sc._jsc.sc().statusTracker().getExecutorInfos()
        total_memory = 0
        total_cores = 0
        for executor in executors:
            print(executor.host())
            print(executor.numRunningTasks())
            # if executor.id() != 'driver':
            total_memory += executor.totalOnHeapStorageMemory()
            print(total_memory)
            print(executor.usedOnHeapStorageMemory())
            print(executor.usedOffHeapStorageMemory())
            # total_cores += executor.totalCores()
        print(f"Job used {total_memory / 1024 / 1024} MB of memory CPU cores")

    def init_io(self, input_path: str, output_path: str):
        if input_path.startswith("s3://"):
            self.in_data_access = S3(
                input_path,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID_IN"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_IN"),
                endpoint_url=os.getenv("AWS_ENDPOINT_URL_IN"),
                service_name="s3",
            )
        else:
            self.in_data_access = Local(input_path)
        if output_path.startswith("s3://"):
            self.out_data_access = S3(
                output_path,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID_OUT"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_OUT"),
                endpoint_url=os.getenv("AWS_ENDPOINT_URL_OUT"),
                service_name="s3",
            )
        else:
            self.out_data_access = Local(output_path)

    def list_files(self, input_path: str, file_ext: str = None) -> tuple[list[dict], dict]:
        file_list, file_stats = self.in_data_access.list_files(input_path, file_ext)
        # sample statistics outputs
        logging.info(f"S3 URL {input_path} has {file_stats.get('total_num_files', 0)} files")
        logging.info(f"File size stats:")
        logging.info(f"  Max size: {file_stats.get('max_file_size', '')}")
        logging.info(f"  Min size: {file_stats.get('min_file_size', '')}")
        logging.info(f"  Average size: {file_stats.get('avg_file_size', '')}")

        return file_list, file_stats

    def set_input_files(self, input_files):
        self.input_files = input_files
        # Retrieve only file names where size is greater than zero
        self.file_names = [
            "s3a://" + file["name"] for file in input_files if file["size"] > 0 and self.data_type in file["name"]
        ]
        self.index = 0

    def get_next_batch(self, batch_size: int) -> int:
        if self.file_index >= len(self.input_files):
            return []  # No more items to retrieve
        batch = []
        end_index = min(self.file_index + batch_size, len(self.input_files))
        for i in range(self.file_index, end_index):
            batch.append(self.input_files[i])
        self.file_index = end_index
        return batch

    def load_metadata(self, metapath: str, task_name: str, transform_name: str) -> tuple[dict, bool]:
        # load_metadata
        meta_filepath = os.path.join(metapath, "metadata.json")
        try:
            metadata_bytes = self.out_data_access.read_file(meta_filepath)
            metadata_string = metadata_bytes.decode("utf-8")
            metadata = json.loads(metadata_string)
        except:
            # failed to read metadata.json (e.g. file was not found),
            # create empty metadata dictionary
            metadata = {}
        batch_records = {}
        if "batch_records" not in metadata:
            metadata["batch_records"] = {}
        else:
            batch_records.update(metadata["batch_records"])
        meta_status = metadata.get("status")
        if meta_status is None:
            meta_status = ""
            metadata["status"] = meta_status
        task_completed = meta_status in ["completed", "completed_all"]
        if task_completed:
            logging.info(f"Found {task_name} for {transform_name} already")
        else:
            logging.info(f"Creating {task_name} for {transform_name}")
        return metadata, task_completed

    def read_data(self, input_data_url: Union[list[str], str], data_type: str) -> DataFrame:
        if (isinstance(input_data_url, str) and input_data_url.startswith("s3://")) or (
            isinstance(input_data_url, list)
            and (input_data_url[0].startswith("s3://") or input_data_url[0].startswith("s3a://"))
        ):
            hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
            access_key = os.getenv("AWS_ACCESS_KEY_ID_IN")
            secret_key = os.getenv("AWS_SECRET_ACCESS_KEY_IN")
            endpoint = os.getenv("AWS_ENDPOINT_URL_IN")
            hconf.set("fs.s3a.access.key", access_key)
            hconf.set("fs.s3a.secret.key", secret_key)
            hconf.set("fs.s3a.endpoint", endpoint)
            logging.info("Applied user-provided credential to S3")

            # for some reason, Hadoop can only process S3 urls if they start with s3a://, not s3://
            if isinstance(input_data_url, str):
                input_data_url = input_data_url.replace("s3://", "s3a://")
            else:
                input_data_url = [x.replace("s3://", "s3a://") for x in input_data_url]
        read_cmd = self.spark.read
        if data_type == ".parquet":
            read_cmd = read_cmd.parquet
        elif data_type == ".csv":
            read_cmd = read_cmd.csv
        elif data_type == ".json":
            read_cmd = read_cmd.json
        elif data_type == ".orc":
            read_cmd = read_cmd.orc
        else:
            read_cmd = read_cmd.text

        if data_type == "rdd":
            spark_df = self.spark.sparkContext.textFile(input_data_url)
        elif isinstance(input_data_url, list):
            spark_df = read_cmd(*input_data_url)
        else:
            spark_df = read_cmd(f"{input_data_url}")
        return spark_df

    def write_data(self, spark_df: DataFrame, output_data_url: str, data_type: str, mode: str = "append"):
        if isinstance(output_data_url, str) and output_data_url.startswith("s3://"):
            hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
            access_key = os.getenv("AWS_ACCESS_KEY_ID_OUT")
            secret_key = os.getenv("AWS_SECRET_ACCESS_KEY_OUT")
            endpoint = os.getenv("AWS_ENDPOINT_URL_OUT")
            hconf.set("fs.s3a.access.key", access_key)
            hconf.set("fs.s3a.secret.key", secret_key)
            hconf.set("fs.s3a.endpoint", endpoint)
            logging.info("Applied user-provided credential to S3")
            # for some reason, Hadoop can only process S3 urls if they start with s3a://, not s3://
            output_data_url = output_data_url.replace("s3://", "s3a://")

        write_cmd = spark_df.write
        if data_type in ["parquet", ".parquet"]:
            write_cmd = write_cmd.parquet
        elif data_type in ["csv", ".csv"]:
            write_cmd = write_cmd.csv
        elif data_type in ["json", ".json"]:
            write_cmd = write_cmd.json
        elif data_type in ["orc", ".orc"]:
            write_cmd = write_cmd.orc
        else:
            write_cmd = write_cmd.text

        write_cmd(output_data_url, mode=mode)

    def stop(self):
        self.spark.stop()


class SparkFileBatcher:
    def __init__(
        self,
        file_list: list[dict],
        batch_size: int,
        data_access: Union[S3, Local],
        checkpoint_file: str,
    ):
        """
        Initializes the SparkFileBatcher class.

        Args:
            files (list[dict]): A list of file dictionaries, with 2 keys: name and size.
            batch_size (int): The number of files to return in each batch.
        """
        self.filepaths = [x["name"] for x in file_list]
        self.num_files = len(self.filepaths)
        self.batch_size = batch_size
        self.file_index = 0
        self.total_file_sizes = [x["size"] for x in file_list]
        self.filelist = file_list
        self.total_batches = (len(self.filepaths) + self.batch_size - 1) // self.batch_size
        self.data_access = data_access
        self.checkpoint_file = checkpoint_file
        self.current_batch_index = self._load_checkpoint()
        self.file_index = self.current_batch_index * self.batch_size

    def next_batch(self) -> list[str]:
        """
        Returns the next batch of files.

        Returns:
            list: A list of file paths for the next batch,
                  or an empty list if all files have been processed.
        """
        if self.file_index >= self.num_files:
            return [], 0  # No more files to process
        batch_end = min(self.file_index + self.batch_size, self.num_files)
        batch = self.filepaths[self.file_index : batch_end]
        self.file_index = batch_end
        # Update current batch index (starts from 1)
        self.current_batch_index = 1 + (self.file_index - 1) // self.batch_size
        return batch, self.get_total_size(batch)

    def get_total_size(self, selected_batch):
        # Create a dictionary from data2 for quick lookup
        size_lookup = {entry["name"]: entry["size"] for entry in self.filelist}

        # Calculate the total size for files in selected_batch
        total_batch_size = sum(size_lookup[file] for file in selected_batch if file in size_lookup)

        return total_batch_size

    def _load_checkpoint(self):
        try:
            checkpoint_bytes = self.data_access.read_file(self.checkpoint_file)
            checkpoint_str = checkpoint_bytes.decode("utf-8")
            current_batch_index = int(checkpoint_str)
        except Exception as ex:
            logging.error(f"Failed to read {self.checkpoint_file}: {ex}")
            current_batch_index = 0
        return current_batch_index

    def _save_checkpoint(self):
        res = self.data_access.write_file(self.checkpoint_file, bytes(str(self.current_batch_index), "utf-8"))
        if res is None:
            logging.error(f"Failed to save checkpoint file {self.checkpoint_file}")
