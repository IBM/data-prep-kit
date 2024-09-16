import argparse
import os
import socket
import time
import traceback
from datetime import datetime

import polars as pl
import yaml
from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.utils import ParamsUtils, get_logger
from file_copy_util import FileCopyUtil
from pyspark.sql import SparkSession


logger = get_logger(__name__)


class FileCopySpark:
    def __init__(self, root_folder: str, num_bands: int, num_segments: int, use_s3: bool):
        self.root_folder = root_folder
        self.num_bands = num_bands
        self.num_segments = num_segments
        self.use_s3 = use_s3
        self.subdirs = [f"band={b}/segment={s}" for b in range(num_bands) for s in range(num_segments)]

    def _init_spark(self, app_name: str = "copy-app") -> SparkSession:
        server_port_https = int(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "-1"))
        if server_port_https == -1:
            # we are running locally
            spark_config = {"spark.driver.host": "127.0.0.1"}
            return SparkSession.builder.appName(app_name).config(map=spark_config).getOrCreate()
        else:
            # we are running in Kubernetes, use spark_profile.yaml and
            # environment variables for configuration

            server_port = os.environ["KUBERNETES_SERVICE_PORT"]
            master_url = f"k8s://https://kubernetes.default:{server_port}"

            # Read Spark configuration profile
            config_filepath = os.path.abspath(
                os.path.join(os.getenv("SPARK_HOME"), "work-dir", "config", "spark_profile.yaml")
            )
            with open(config_filepath, "r") as config_fp:
                spark_config = yaml.safe_load(os.path.expandvars(config_fp.read()))
            spark_config["spark.submit.deployMode"] = "client"

            # configure the executor pods from template
            executor_pod_template_file = os.path.join(
                os.getenv("SPARK_HOME"),
                "work-dir",
                "src",
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
            logger.info(f"Launching Spark Session with configuration\n" f"{yaml.dump(spark_config, indent=2)}")
            app_name = spark_config.get("spark.app.name", "my-spark-app")
            return SparkSession.builder.master(master_url).appName(app_name).config(map=spark_config).getOrCreate()

    def create_data_access_factory(self, root_folder: str, use_s3: bool) -> DataAccessFactoryBase:
        input_folder = root_folder
        output_folder = root_folder
        data_access_factory: DataAccessFactoryBase = DataAccessFactory()
        daf_args = []
        if args.use_s3:
            s3_creds = {
                "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "url": os.getenv("AWS_ENDPOINT_URL"),
            }
            s3_config = {
                "input_folder": root_folder,
                "output_folder": root_folder,
            }
            daf_args.append("--data_s3_cred")
            daf_args.append(ParamsUtils.convert_to_ast(s3_creds))
            daf_args.append("--data_s3_config")
            daf_args.append(ParamsUtils.convert_to_ast(s3_config)),
        else:
            local_config = {
                "input_folder": root_folder,
                "output_folder": os.path.join(root_folder, "bands_consolidated"),
            }
            daf_args.append("--data_local_config")
            daf_args.append(ParamsUtils.convert_to_ast(local_config))
        daf_parser = argparse.ArgumentParser()
        data_access_factory.add_input_params(parser=daf_parser)
        data_access_factory_args = daf_parser.parse_args(args=daf_args)
        data_access_factory.apply_input_params(args=data_access_factory_args)

        return data_access_factory

    def orchestrate(
        self,
        runtime_config: dict,
        execution_config: dict,
        data_access_factory: DataAccessFactoryBase,
    ) -> int:
        """
        orchestrator for transformer execution
        :param execution_config: orchestrator configuration
        :param data_access_factory: data access factory
        :param runtime_config: transformer runtime configuration
        :return: 0 - success or 1 - failure
        """
        start_time = time.time()
        start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"orchestrator started at {start_ts}")
        data_access = data_access_factory.create_data_access()
        # initialize Spark
        spark_session = self._init_spark()
        sc = spark_session.sparkContext
        transform_config = sc.broadcast(runtime_config)
        daf = sc.broadcast(data_access_factory)

        def process_partition(iterator):
            """
            process partitions
            :param iterator: iterator of records
            :return:
            """
            # local statistics dictionary
            stats = {}
            # create file processor
            file_processor = FileCopyUtil(
                data_access_factory=daf.value,
                config=transform_config.value,
                stats=stats,
            )
            for f in iterator:
                stats = file_processor.copy_data(subfolder_name=f[0])
            # return partition's statistics
            return list(stats.items())

        num_partitions = 0
        try:
            # Get files to process
            files = [
                f"band={band}/segment={segment}"
                for band in range(self.num_bands)
                for segment in range(self.num_segments)
            ]
            if len(files) == 0:
                logger.error("No input files to process - exiting")
                return 0
            logger.info(f"Number of files is {len(files)}")
            # process data
            logger.debug("Begin processing files")
            source_rdd = sc.parallelize(files, execution_config.get("parallelization"))
            num_partitions = source_rdd.getNumPartitions()
            logger.info(f"Parallelizing execution. Using {num_partitions} partitions")
            stats_rdd = source_rdd.zipWithIndex().mapPartitions(process_partition)
            # build overall statistics
            stats = dict(stats_rdd.reduceByKey(lambda a, b: a + b).collect())
            return_code = 0
            status = "success"
        except Exception as e:
            # process execution exception
            logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
            return_code = 1
            status = "failure"
            stats = {}
        try:
            # build and save metadata
            logger.debug("Building job metadata")
            input_params = runtime_config
            # input_params = runtime_config.get_transform_metadata() | execution_config.get_input_params()
            metadata = {
                "job details": {
                    "start_time": start_ts,
                    "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status": status,
                },
                "job_input_params": input_params | data_access_factory.get_input_params(),
                "execution_stats": {
                    "num partitions": num_partitions,
                    "execution time, min": (time.time() - start_time) / 60,
                },
                "job_output_stats": stats,
            }
            logger.debug(f"Saving job metadata: {metadata}.")

            if data_access_factory.s3_config is not None:
                _, root_folder = self.root_folder.split("://")
                in_path = os.path.join(root_folder, "bands")
                out_path = os.path.join(root_folder, "bands_consolidated")
                data_access.input_folder = f"{in_path}{os.sep}"
                data_access.output_folder = f"{out_path}{os.sep}"
            else:
                data_access.input_folder = os.path.join(self.root_folder, "bands")
                data_access.output_folder = os.path.join(self.root_folder, "bands_consolidated")
            data_access.save_job_metadata(metadata)
            logger.debug("Saved job metadata.")
            return return_code
        except Exception as e:
            logger.error(f"Exception during execution {e}: {traceback.print_exc()}")
            return 1
        finally:
            # stop spark context at the end. Required for running multiple tests
            spark_session.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root_folder",
        type=str,
        default=os.getenv("HOME"),
        help="root folder",
    )
    parser.add_argument(
        "--num_bands",
        type=int,
        default=0,
        help="number of bands",
    )
    parser.add_argument(
        "--num_segments",
        type=int,
        default=0,
        help="number of segments",
    )
    parser.add_argument(
        "--parallelization",
        type=int,
        default=-1,
        help="spark parallelization",
    )
    parser.add_argument(
        "--use_s3",
        type=bool,
        default=False,
        help="use s3",
    )
    args = parser.parse_args()
    fcs = FileCopySpark(args.root_folder, args.num_bands, args.num_segments, args.use_s3)
    data_access_factory = fcs.create_data_access_factory(args.root_folder, args.use_s3)
    app_config = {"root_folder": args.root_folder}
    execution_config = {"parallelization": args.parallelization} if args.parallelization > 0 else {}
    status = fcs.orchestrate(app_config, execution_config, data_access_factory)
    print(f"Orchestrate concluded with status {status}")
