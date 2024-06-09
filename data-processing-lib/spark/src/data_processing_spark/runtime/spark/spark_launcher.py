# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import os
import socket
from argparse import ArgumentParser
from typing import Union

import yaml
from data_processing.data_access import (
    DataAccess,
    DataAccessFactory,
    DataAccessFactoryBase,
    DataAccessS3,
)
from data_processing.runtime import (
    AbstractTransformLauncher,
    TransformExecutionConfiguration,
)
from data_processing.utils import get_logger, str2bool
from data_processing_spark.runtime.spark.runtime_config import (
    SparkTransformRuntimeConfiguration,
)
from data_processing_spark.runtime.spark.spark_execution_config import (
    SparkExecutionConfiguration,
)
from data_processing_spark.runtime.spark.spark_transform import AbstractSparkTransform
from pyspark.sql import DataFrame, SparkSession


logger = get_logger(__name__)


class SparkTransformLauncher(AbstractTransformLauncher):
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        runtime_config: SparkTransformRuntimeConfiguration,
        data_access_factory: DataAccessFactoryBase = DataAccessFactory(),
    ):
        """
        Creates driver
        :param runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        super().__init__(runtime_config, data_access_factory)
        self.runtime_config = runtime_config
        self.execution_config = SparkExecutionConfiguration(runtime_config.get_name())

    def launch(self):
        if not self._get_args():
            logger.warning("Arguments could not be applied.")
            return 1
        transform_params = dict(self.runtime_config.get_transform_params())
        transform_class = self.runtime_config.get_transform_class()
        transform = transform_class(transform_params)
        data_access = self.data_access_factory.create_data_access()
        self._start_spark()
        try:
            self._run_transform(data_access, transform)
        except Exception as ex:
            logger.error(f"Failed to run transform: {ex}")
        finally:
            self._stop_spark()

    def _start_spark(self):
        server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
        if server_port_https == -1:
            # we are running locally, use the spark_profile_local.yaml file for configuration
            config_filepath = self.execution_config.local_config_filepath
            with open(config_filepath, "r") as config_fp:
                spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
            app_name = spark_config.get("spark.app.name", "my-spark-app")
            self.spark = SparkSession.builder.appName(app_name).config(map=spark_config).getOrCreate()
        else:
            logger.info("Kubernetes launch currently work in progress")

        #     # we are running in Kubernetes, use spark_profile.yaml and
        #     # environment variables for configuration

        #     server_port = os.environ["KUBERNETES_SERVICE_PORT"]
        #     master_url = f"k8s://https://kubernetes.default:{server_port}"

        #     # Read Spark configuration profile
        #     config_filepath = self.execution_config.kube_config_filepath
        #     with open(config_filepath, "r") as config_fp:
        #         spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
        #     spark_config["spark.submit.deployMode"] = "client"

        #     # configure the executor pods from template
        #     # todo: put this file in the pypi wheel
        #     logger.warning("TODO: Need to define location of pod template!")
        #     executor_pod_template_file = os.path.join(
        #         os.path.dirname(__file__),
        #         "templates",
        #         "spark-executor-pod-template.yaml",
        #     )
        #     spark_config["spark.kubernetes.executor.podTemplateFile"] = executor_pod_template_file
        #     spark_config["spark.kubernetes.container.image.pullPolicy"] = "Always"

        #     # Pass the driver IP address to the workers for callback
        #     myservice_url = socket.gethostbyname(socket.gethostname())
        #     spark_config["spark.driver.host"] = myservice_url
        #     spark_config["spark.driver.bindAddress"] = "0.0.0.0"

        #     spark_config["spark.decommission.enabled"] = True

        #     logger.info(f"Launching Spark Session with configuration\n" f"{yaml.dump(spark_config, indent=2)}")
        #     app_name = spark_config.get("spark.app.name", "my-spark-app")
        #     self.spark = (
        #         SparkSession.builder.master(master_url).appName(app_name).config(map=spark_config).getOrCreate()
        #     )

        # # configure S3 for Spark Session
        # hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
        # hconf.set("com.amazonaws.services.s3.enableV4", "true")
        # hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # # hconf.set(
        # #     "fs.s3a.aws.credentials.provider",
        # #     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        # # )
        # hconf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    def _stop_spark(self):
        self.spark.stop()

    def _get_io_env_var(self, base_name: str, for_input: bool):
        """
        Get the env var that may or may not exist with _IN or _OUT suffix.
        First try with the suffix using the given base_name.  If found return its value,
        otherwise get the the env var value for the given base_name.
        The idea is to allow the non-suffixed value to serve as a default for both *_IN and *_OUT
        env vars.
        Args:
            base_name:
            for_input:

        Returns: None if none found.
        """
        if for_input:
            io_var = base_name + +"_IN"
        else:
            io_var = base_name + +"_OUT"
        value = os.getenv(io_var)
        if value is None:
            value = os.getenv(base_name)
        return value

    def _apply_s3_credentials(self, for_input: bool, data_access: DataAccess) -> None:
        """
        Consult AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY and AWS_ENDPOINT_URL
        w/ and w/o corresponding _IN or _OUT suffix and set the values (if found)
        into the corresponding 'fs.s3a.*' settings for hadoop configuration of
        this instance's spark session.
        Args:
            for_input: true if settings are for reads, false is for writes.
        """

        hconf = self.spark.sparkContext._jsc.hadoopConfiguration()
        if for_input:
            input_name = "input"
        else:
            input_name = "output"
        data_access_s3 = data_access if isinstance(data_access, DataAccessS3) else None

        value = None if data_access_s3 is None else data_access_s3.get_access_key()
        if value is None:
            value = self._get_io_env_var("AWS_ACCESS_KEY_ID", for_input)
        if value is not None:
            logger.info(f"Applying user-provided S3 {input_name} access key: {value}")
            hconf.set("fs.s3a.access.key", value)

        value = None if data_access_s3 is None else data_access_s3.get_secret_key()
        if value is None:
            value = self._get_io_env_var("AWS_SECRET_ACCESS_KEY", True)
        if value is not None:
            logger.info(f"Applying user-provided S3 {input_name} secret key")
            hconf.set("fs.s3a.secret.key", value)

        value = None if data_access_s3 is None else data_access_s3.get_endpoint()
        if value is None:
            value = self._get_io_env_var("AWS_ENDPOINT_URL", True)
        if value is not None:
            logger.info(f"Applying user-provided S3 {input_name} end point: {value}")
            hconf.set("fs.s3a.endpoint", value)

    def _read_data(self, data_access: DataAccess) -> DataFrame:
        input_data_url, _, _ = data_access.get_files_to_process()
        logger.info(f"files = {input_data_url}")
        data_type = "parquet"  # This should come from the extensions in data_access,

        if (isinstance(input_data_url, str) and input_data_url.startswith("s3://")) or (
            isinstance(input_data_url, list) and len(input_data_url) > 0 and input_data_url[0].startswith("s3://")
        ):
            self._apply_s3_credentials(True, data_access)
            # for some reason, Hadoop can only process S3 urls if they start with s3a://, not s3://
            if isinstance(input_data_url, str):
                input_data_url = input_data_url.replace("s3://", "s3a://")
            else:
                input_data_url = [x.replace("s3://", "s3a://") for x in input_data_url]

        read_cmd = self.spark.read
        if data_type == "parquet":
            read_cmd = read_cmd.parquet
        elif data_type == "csv":
            read_cmd = read_cmd.csv
        elif data_type == "json":
            read_cmd = read_cmd.json
        elif data_type == "orc":
            read_cmd = read_cmd.orc
        else:
            read_cmd = read_cmd.text

        spark_df = read_cmd(*input_data_url)
        return spark_df

    def _write_data(self, spark_df: DataFrame, data_access: DataAccess):
        data_type = "parquet"  # Todo: this should come from data access.
        output_data_url = data_access.get_output_folder()
        if isinstance(output_data_url, str) and output_data_url.startswith("s3://"):
            self._apply_s3_credentials(False, data_access)
            # for some reason, Hadoop can only process S3 urls if they start with s3a://, not s3://
            output_data_url = output_data_url.replace("s3://", "s3a://")

        write_cmd = spark_df.write.mode("overwrite")
        if data_type == "parquet":
            write_cmd = write_cmd.parquet
        elif data_type == "csv":
            write_cmd = write_cmd.csv
        elif data_type == "json":
            write_cmd = write_cmd.json
        elif data_type == "orc":
            write_cmd = write_cmd.orc
        else:
            write_cmd = write_cmd.text
        write_cmd(output_data_url)

    def _run_transform(self, data_access: DataAccess, transform: AbstractSparkTransform):
        spark_df = self._read_data(data_access)
        res_spark_df, metadata = transform.transform(spark_df)
        self._write_data(res_spark_df[0], data_access)
        data_access.save_job_metadata(metadata)

    def _get_args(self) -> bool:
        parser = argparse.ArgumentParser(
            description=f"Driver for {self.name} processing on Spark",
            # RawText is used to allow better formatting of ast-based arguments
            # See uses of ParamsUtils.dict_to_str()
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self._add_input_params(parser)
        args = parser.parse_args()
        return self._apply_input_params(args)

    def _add_input_params(self, parser):

        # parser.add_argument(
        #     "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="Run Spark locally "
        # )
        # add additional arguments
        self.runtime_config.add_input_params(parser=parser)
        self.data_access_factory.add_input_params(parser=parser)
        self.execution_config.add_input_params(parser=parser)

    def _apply_input_params(self, args: argparse.Namespace):
        # self.run_locally = args.run_locally
        # if self.run_locally:
        #     logger.info("Running locally")
        # else:
        #     logger.info("connecting to existing cluster")
        return (
            self.runtime_config.apply_input_params(args=args)
            and self.data_access_factory.apply_input_params(args=args)
            and self.execution_config.apply_input_params(args=args)
        )
