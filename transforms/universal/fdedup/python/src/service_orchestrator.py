import argparse
import os
import sys

from cluster_analysis_transform_python import (
    ClusterAnalysisPythonTransformConfiguration,
)
from data_cleaning_transform_python import DataCleaningPythonTransformConfiguration
from data_processing.data_access import DataAccessFactory, DataAccessFactoryBase
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import ParamsUtils
from file_copy_util import FileCopyUtil
from signature_calc_transform_python import (
    SignatureCalculationPythonTransformConfiguration,
)


class ServiceOrchestrator:
    def __init__(self, global_params=None):
        self.global_params = global_params or {}

    def execute_service(self, service_logic, service_params):
        # Call the generic service logic
        service_logic(service_params)

    def orchestrate(self, service_logic):
        service_list = self.global_params["services"].split(",")

        for service in service_list:
            if service == "SignatureCalculation":
                params = create_transform_args_payload(args, service)
                params["service_type"] = "SignatureCalculation"
                self.execute_service(service_logic, params)
            elif service == "ClusterAnalysis":
                params = create_transform_args_payload(args, service)
                params["service_type"] = "ClusterAnalysis"
                self.execute_service(service_logic, params)
            elif service == "DataCleaning":
                params = create_transform_args_payload(args, service)
                params["service_type"] = "DataCleaning"
                self.execute_service(service_logic, params)
            elif service == "BandsFileCopy":
                params = args
                params["service_type"] = "BandsFileCopy"
                self.execute_service(service_logic, params)
            elif service == "DocsToRemoveFileCopy":
                params = args
                params["service_type"] = "DocsToRemoveFileCopy"
                self.execute_service(service_logic, params)
            else:
                print(f"Warning: {service} is not a recognized service.")


def generic_service_logic(params):
    print("Service executed with parameters:", params)
    service_type = params["service_type"]
    use_s3 = params["use_s3"]
    # Remove the 'service_type' key
    params.pop("service_type", None)  # Using pop() method

    if service_type == "SignatureCalculation" or service_type == "ClusterAnalysis" or service_type == "DataCleaning":
        # Set the simulated command line args
        params.pop("num_permutations", None)  # Using pop() method
        params.pop("num_bands", None)  # Using pop() method
        params.pop("num_segments", None)  # Using pop() method
        params.pop("use_s3", None)  # Using pop() method
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    if use_s3:
        sys.argv.append("--data_s3_cred")
        sys.argv.append(ParamsUtils.convert_to_ast(s3_creds))

    if service_type == "SignatureCalculation":
        runtime_config = SignatureCalculationPythonTransformConfiguration()
        launch_transform_service(runtime_config)
    elif service_type == "ClusterAnalysis":
        runtime_config = ClusterAnalysisPythonTransformConfiguration()
        launch_transform_service(runtime_config)
    elif service_type == "DataCleaning":
        runtime_config = DataCleaningPythonTransformConfiguration()
        launch_transform_service(runtime_config)
    elif service_type == "BandsFileCopy":
        launch_file_copy_service(params, service_type)
    elif service_type == "DocsToRemoveFileCopy":
        launch_file_copy_service(params, service_type)


def launch_transform_service(params):
    # create launcher
    launcher = PythonTransformLauncher(runtime_config=params)
    # Launch the ray actor(s) to process the input
    launcher.launch()


def launch_file_copy_service(args, service_type):
    root_folder = os.path.join(args["root_folder"], args["output_folder"])
    data_type = None
    if service_type == "BandsFileCopy":
        data_type = "bands"
        # Get files to process
        files = [
            f"band={band}/segment={segment}"
            for band in range(args["num_bands"])
            for segment in range(args["num_segments"])
        ]
    elif service_type == "DocsToRemoveFileCopy":
        files = ["docs_to_remove"]
        data_type = "docs_to_remove"
    config = {"root_folder": root_folder}
    data_access_factory: DataAccessFactoryBase = DataAccessFactory()
    daf_args = []

    if args["use_s3"]:

        s3_config = {
            "input_folder": root_folder,
            "output_folder": root_folder,
        }
        daf_args.append("--data_s3_cred")
        daf_args.append(ParamsUtils.convert_to_ast(s3_creds))
        daf_args.append("--data_s3_config")
        daf_args.append(ParamsUtils.convert_to_ast(s3_config)),
    else:

        # Construct folders
        local_config = {
            "input_folder": root_folder,
            "output_folder": os.path.abspath(os.path.join(args["root_folder"], args["output_folder"])),
        }
        daf_args.append("--data_local_config")
        daf_args.append(ParamsUtils.convert_to_ast(local_config))

    daf_parser = argparse.ArgumentParser()
    data_access_factory.add_input_params(parser=daf_parser)
    data_access_factory_args = daf_parser.parse_args(args=daf_args)
    data_access_factory.apply_input_params(args=data_access_factory_args)
    stats = {}
    fcu = FileCopyUtil(data_access_factory=data_access_factory, config=config, stats=stats)
    for file in files:
        fcu.copy_data(file, data_type)


def create_transform_args_payload(args, service):
    print(args)
    # Construct folders
    input_folder = os.path.join(args["root_folder"], args["input_folder"])
    output_folder = os.path.join(args["root_folder"], args["output_folder"])
    if service == "ClusterAnalysis":
        input_folder = os.path.join(args["root_folder"], args["output_folder"], "bands_consolidated")
        output_folder = os.path.join(args["root_folder"], args["output_folder"], "docs_to_remove")
    elif service == "DataCleaning":
        output_folder = os.path.join(args["root_folder"], args["output_folder"], "cleaned")
        duplicate_location = os.path.join(
            args["root_folder"],
            args["output_folder"],
            "docs_to_remove_consolidated",
            "docs_to_remove_consolidated.parquet",
        )

    # Create a local configuration
    local_conf = {"input_folder": input_folder, "output_folder": output_folder}

    # Create parameters
    params = {
        "num_permutations": args["num_permutations"],
        "num_bands": args["num_bands"],
        "num_segments": args["num_segments"],
        "use_s3": args["use_s3"],
    }

    if args["use_s3"]:
        params["data_s3_config"] = ParamsUtils.convert_to_ast(local_conf)
    else:
        params["data_local_config"] = ParamsUtils.convert_to_ast(local_conf)

    # add extra
    if service == "DataCleaning":
        short_name = "fdclean"
        cli_prefix = f"{short_name}_"

        # configuration keys
        document_id_column_key = "document_id_column"
        """ This key holds the name of the column storing the unique ID assigned to each document"""
        duplicate_list_location_key = "duplicate_list_location"
        """ This key holds the location of the list of duplicate documents marked for removal"""

        # command line arguments
        document_id_column_cli_param = f"{cli_prefix}{document_id_column_key}"
        """ Name of the column storing the unique ID assigned to each document"""
        duplicate_list_location_cli_param = f"{cli_prefix}{duplicate_list_location_key}"
        """ Location of the list of duplicate documents marked for removal"""

        params[document_id_column_cli_param] = "int_id_column"
        params[duplicate_list_location_cli_param] = duplicate_location

    return params


def create_file_copy_args_payload(args):
    daf_args = []
    local_config = {
        "input_folder": args.root_folder,
        "output_folder": args.root_folder,
    }
    daf_args.append("--data_local_config")
    daf_args.append(ParamsUtils.convert_to_ast(local_config))
    data_access_factory: DataAccessFactoryBase = DataAccessFactory()
    daf_parser = argparse.ArgumentParser()
    data_access_factory.add_input_params(parser=daf_parser)
    data_access_factory_args = daf_parser.parse_args(args=daf_args)
    data_access_factory.apply_input_params(args=data_access_factory_args)
    return data_access_factory


def parse_args():
    parser = argparse.ArgumentParser(description="Service Orchestrator")

    # Define command line arguments
    parser.add_argument("--root_folder", type=str, required=True, help="Root folder path")
    parser.add_argument("--input_folder", type=str, required=True, help="Input folder path")
    parser.add_argument("--output_folder", type=str, required=True, help="Output folder path")

    parser.add_argument("--num_permutations", type=int, default=112, help="Number of permutations")
    parser.add_argument("--num_bands", type=int, default=14, help="Number of bands")
    parser.add_argument("--num_segments", type=int, default=2, help="Number of segments")

    # Single argument for service execution
    parser.add_argument(
        "--services",
        type=str,
        required=True,
        help="Comma-separated list of services to run (e.g., SignatureCalculation,BandsFileCopy,ClusterAnalysis,DocsToRemoveFileCopy,DataCleaning)",
    )

    parser.add_argument(
        "--use_s3",
        type=bool,
        default=False,
        help="use s3",
    )

    args = parser.parse_args()
    return vars(args)  # Convert Namespace to dictionary


if __name__ == "__main__":

    s3_creds = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "url": os.getenv("AWS_ENDPOINT_URL"),
    }

    # Parse command line arguments
    args = parse_args()

    # Initialize the orchestrator
    orchestrator = ServiceOrchestrator(global_params=args)

    # Example service execution (if you had defined services)
    orchestrator.orchestrate(generic_service_logic)
