# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import logging
import os
import sys
from typing import Union

import polars as pl
from cluster_analysis_transform_spark import ClusterAnalysisSparkTransformConfiguration
from data_cleaning_transform_spark import DataCleaningSparkTransformConfiguration
from data_processing.utils import ParamsUtils
from data_processing_spark.runtime.spark import SparkTransformLauncher
from file_copy_util import FileCopyUtil
from file_copy_util_spark import FileCopySpark
from signature_calc_transform_spark import (
    SignatureCalculationSparkTransformConfiguration,
)


s3_creds = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "url": os.getenv("AWS_ENDPOINT_URL"),
}

args_map = {
    "minhash": [
        "document_id_column",
        "contents_column",
        "seed",
        "num_permutations",
        "num_bands",
        "num_minhashes_per_band",
        "jaccard_similarity_threshold",
        "word_shingle_size",
        "num_segments",
    ],
    "copyutil": [
        "subfolder_name",
        "data_type",
        "num_bands",
        "num_segments",
        "parallelization",
        "use_s3",
    ],
    "cluster": [
        "jaccard_similarity_threshold",
    ],
    "fdclean": [
        "document_id_column",
        "duplicate_list_location",
    ],
}


def get_arguments(in_args: argparse.Namespace, module_name: str) -> Union[list, dict]:
    sys_argv = ["python"]
    in_args_dict = vars(in_args)
    if in_args.use_s3:
        sys_argv.append("--data_s3_cred")
        sys_argv.append(ParamsUtils.convert_to_ast(s3_creds))
    all_module_arguments = args_map.get(module_name, [])
    passed_args = {k: v for k, v in in_args_dict.items() if k in all_module_arguments and v is not None}
    if module_name == "copyutil":
        copy_util_config = {k: v for k, v in passed_args.items()}
        copy_util_config["root_folder"] = in_args_dict["output_folder"]
        return copy_util_config
    else:
        for k, v in passed_args.items():
            sys_argv.append(f"--{module_name}_{k}")
            sys_argv.append(str(v))
        if module_name == "minhash":
            input_folder = in_args_dict["input_folder"]
            output_folder = os.path.join(in_args_dict["output_folder"])
        elif module_name == "cluster":
            input_folder = os.path.join(in_args_dict["output_folder"], "bands_consolidated")
            output_folder = os.path.join(in_args_dict["output_folder"], "docs_to_remove")
        elif module_name == "fdclean":
            if f"--{module_name}_duplicate_list_location" not in sys_argv:
                sys_argv.append(f"--{module_name}_duplicate_list_location")
                sys_argv.append(
                    os.path.join(
                        in_args_dict["output_folder"],
                        "docs_to_remove_consolidated",
                        "docs_to_remove_consolidated.parquet",
                    )
                )
            input_folder = in_args_dict["input_folder"]
            output_folder = os.path.join(in_args_dict["output_folder"], "cleaned")
        else:
            logging.error(f"Unknown module name: {module_name}")
        data_io = {
            "input_folder": input_folder,
            "output_folder": output_folder,
        }
        if in_args.use_s3:
            sys_argv.append("--data_s3_config")
        else:
            sys_argv.append("--data_local_config")
        sys_argv.append(ParamsUtils.convert_to_ast(data_io))
    return sys_argv


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_folder", type=str, required=True, help="path to read the input files")
    parser.add_argument("--output_folder", type=str, required=True, help="path to write the output files")
    parser.add_argument(
        "--use_s3", type=bool, required=False, default=False, help="if true, use S3, if false use local FS"
    )
    parser.add_argument(
        "--contents_column", type=str, required=False, help="name of the column that stores document text"
    )
    parser.add_argument(
        "--document_id_column", type=str, required=False, help="name of the column that stores document text"
    )
    parser.add_argument("--seed", type=int, required=False, help="name of the column that stores document text")
    parser.add_argument(
        "--num_permutations", type=int, required=True, help="number of permutations to use for minhash calculation"
    )
    parser.add_argument(
        "--num_bands", type=int, required=True, help="number of bands to use for band hash calculation"
    )
    parser.add_argument(
        "--num_minhashes_per_band", type=int, required=True, help="number of minhashes to use in each band"
    )
    parser.add_argument(
        "--word_shingle_size", type=int, required=False, help="number of words included in one shingle"
    )
    parser.add_argument(
        "--jaccard_similarity_threshold",
        type=float,
        required=False,
        help="jaccard similarity threshold above which two documents are similar",
    )
    parser.add_argument(
        "--num_segments",
        type=int,
        required=True,
        help="number of segments to divide each band hash interval (to improve scalability)",
    )
    parser.add_argument("--parallelization", type=int, required=False, default=-1, help="spark parallelization")
    parser.add_argument(
        "--duplicate_list_location",
        type=str,
        required=False,
        help="path to the file with all the duplicate document ids",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # configure logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    args = parse_arguments()
    sys.argv = get_arguments(args, "minhash")
    # create launcher
    launcher = SparkTransformLauncher(runtime_config=SignatureCalculationSparkTransformConfiguration())
    # Launch the spark worker(s) to process the input
    status = launcher.launch()
    logging.info(f"Signature calculation concluded with status {status}")

    fcs_config = get_arguments(args, "copyutil")

    root_folder = fcs_config["root_folder"]
    parallelization = fcs_config["parallelization"]
    fcs = FileCopySpark(root_folder, fcs_config["num_bands"], fcs_config["num_segments"], args.use_s3)
    data_access_factory = fcs.create_data_access_factory(root_folder, args.use_s3)
    app_config = {"root_folder": root_folder}
    execution_config = {"parallelization": parallelization} if parallelization > 0 else {}
    status = fcs.orchestrate(app_config, execution_config, data_access_factory, data_type="bands")
    logging.info(f"Consolidate bands concluded with status {status}")

    sys.argv = get_arguments(args, "cluster")
    launcher = SparkTransformLauncher(runtime_config=ClusterAnalysisSparkTransformConfiguration())
    # Launch the spark worker(s) to process the input
    status = launcher.launch()
    logging.info(f"Cluster analysis concluded with status {status}")

    stats = {}
    fcu_config = get_arguments(args, "copyutil")
    fcu = FileCopyUtil(data_access_factory=data_access_factory, config=fcu_config, stats=stats)
    fcu.copy_data(subfolder_name="docs_to_remove", data_type="docs_to_remove")

    sys.argv = get_arguments(args, "fdclean")
    # create launcher
    launcher = SparkTransformLauncher(runtime_config=DataCleaningSparkTransformConfiguration())
    # Launch the spark worker(s) to process the input
    status = launcher.launch()
    logging.info(f"Data cleanup concluded with status {status}")
