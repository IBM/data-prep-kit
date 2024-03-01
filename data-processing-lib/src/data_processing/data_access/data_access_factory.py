import argparse
import ast
from typing import Any

from data_processing.data_access import (
    DataAccess,
    DataAccessLakeHouse,
    DataAccessLocal,
    DataAccessS3,
)
from data_processing.utils import CLIArgumentProvider, ParamsUtils, get_logger, str2bool


logger = get_logger(__name__)


class DataAccessFactory(CLIArgumentProvider):
    """
    This class is accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self):
        """
        Initialization - set defaults
        """
        self.s3_config = None
        self.lh_config = None
        self.local_config = None
        self.s3_cred = None
        self.checkpointing = False
        self.dsets = None
        self.max_files = -1

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Define data access specific parameters
        The set of parameters here is a superset of parameters required for all
        supported data access. The user only needs to specify the ones that he needs
        the rest will have the default values
        This might need to be extended if new data access implementation is added
        :param parser: parser
        :return: None
        """

        help_example_dict = {
            "access_key": ["AFDSASDFASDFDSF ", "access key help text"],
            "secret_key": ["XSDFYZZZ", "secret key help text"],
            "cos_url": ["s3:/cos-optimal-llm-pile/test/", "COS url"],
        }
        parser.add_argument(
            "--s3_cred",
            type=ast.literal_eval,
            default=None,
            help="AST string of options for cos credentials. Only required for COS or Lakehouse.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        help_example_dict = {
            "input_path": [
                "/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup",
                "Path to input folder of files to be processed",
            ],
            "output_path": [
                "/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed",
                "Path to outpu folder of processed files",
            ],
        }
        parser.add_argument(
            "--s3_config",
            type=ast.literal_eval,
            default=None,
            help="AST string containing input/output paths.\n" + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        help_example_dict = {
            "input_table": [
                "/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup",
                "Path to input folder of files to be processed",
            ],
            "input_dataset": [
                "/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed",
                "Path to outpu folder of processed files",
            ],
            "input_version": ["1.0", "Version number to be associated with the input."],
            "output_table": ["ededup", "Name of table into which data is written"],
            "output_path": [
                "/cos-optimal-llm-pile/bluepile-processing/rel0_8/cc15_30_preproc_ededup/processed",
                "Path to output folder of processed files",
            ],
            "token": ["AASDFZDF", "The token to use for Lakehouse authentication"],
            "lh_environment": ["STAGING", "Operational environment. One of STAGING or PROD"],
        }
        parser.add_argument(
            "--lh_config",
            type=ast.literal_eval,
            default=None,
            help="AST string containing input/output using lakehouse.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        help_example_dict = {
            "input_folder": ["./input", "Path to input folder of files to be processed"],
            "output_folder": ["/tmp/output", "Path to output folder of processed files"],
        }
        parser.add_argument(
            "--local_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing input/output folders using local fs.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument("--max_files", type=int, default=-1, help="Max amount of files to process")
        parser.add_argument(
            "--checkpointing", type=lambda x: bool(str2bool(x)), default=False, help="checkpointing flag"
        )
        parser.add_argument("--data_sets", type=str, default=None, help="List of data sets")

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate data access specific parameters
        This might need to be extended if new data access implementation is added
        :param args: user defined arguments
        :return: None
        """

        # check which configuration (S3, LakeHouse, or Local) is specified
        s3_config_specified = 1 if args.s3_config is not None and len(args.s3_config) > 1 else 0
        lh_config_specified = 1 if args.lh_config is not None and len(args.lh_config) > 1 else 0
        local_config_specified = 1 if args.local_config is not None and len(args.local_config) > 1 else 0

        # check that only one (S3, LakeHouse, or Local) configuration is specified
        if s3_config_specified + lh_config_specified + local_config_specified > 1:
            logger.error(
                f"{'S3, ' if s3_config_specified == 1 else ''}"
                f"{'Lakehouse, ' if lh_config_specified == 1 else ''}"
                f"{'Local ' if local_config_specified == 1 else ''}"
                "configurations specified, but only one configuration expected"
            )
            return False

        # check that at least one (S3, LakeHouse, or Local) configuration is specified
        if s3_config_specified + lh_config_specified + local_config_specified == 0:
            logger.error(
                "No S3, lakehouse, or local configuration parameters defined," " at least one of them is required! "
            )
            return False

        # further validate the specified configuration (S3, LakeHouse, or Local)
        if s3_config_specified == 1:
            if not self.__validate_s3_cred(s3_credentials=args.s3_cred):
                return False
            self.s3_cred = args.s3_cred
            self.s3_config = args.s3_config
            logger.info(
                f'Using s3 configuration: input path - {self.s3_config["input_folder"]}, '
                f'output path - {self.s3_config["output_folder"]}'
            )
        elif lh_config_specified == 1:
            if not self.__validate_s3_cred(s3_credentials=args.s3_cred):
                return False
            self.s3_cred = args.s3_cred
            self.lh_config = args.lh_config
            logger.info(
                f'Using lake house configuration: input table - {self.lh_config["input_table"]}, '
                f'input_dataset - {self.lh_config["input_dataset"]}, '
                f'input_version - {self.lh_config["input_version"]}, '
                f'output table - {self.lh_config["output_table"]}, '
                f'output_path - {self.lh_config["output_path"]}, '
                f'lh_environment - {self.lh_config["lh_environment"]} '
            )
        elif local_config_specified == 1:
            if not self._validate_local(local_config=args.local_config):
                return False
            self.local_config = args.local_config
            logger.info(
                f"Using local configuration with: "
                f"input_folder - {self.local_config['input_folder']} "
                f"output_folder - {self.local_config['output_folder']}"
            )
        self.checkpointing = args.checkpointing
        self.max_files = args.max_files
        if args.data_sets is None or len(args.data_sets) < 1:
            logger.info(f"Not using data sets, checkpointing {self.checkpointing}, max files {self.max_files}")
        else:
            self.dsets = args.data_sets.split(",")
            logger.info(
                f"Using data sets {self.dsets}, checkpointing {self.checkpointing}, max files {self.max_files}"
            )
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params for metadata
        :return: dictionary of params
        """
        params = {
            "checkpointing": self.checkpointing,
            "max_files": self.max_files,
        }
        if self.dsets is not None:
            params["data sets"] = self.dsets
        return params

    @staticmethod
    def __validate_s3_cred(s3_credentials: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_credentials: dictionary of S3 credentials
        :return:
        """
        if s3_credentials is None:
            logger.error("Could not get cos credentials - exiting")
            return False
        if (
            s3_credentials.get("access_key", "") == ""
            or s3_credentials.get("secret_key", "") == ""
            or s3_credentials.get("cos_url", "") == ""
        ):
            logger.error("Could not get cos credentials - exiting")
            return False
        return True

    @staticmethod
    def _validate_local(local_config: dict[str, str]) -> bool:
        """
        Validate that
        :param local_config: dictionary of local config
        :return: True if local config is valid, False otherwise
        """
        if local_config is None:
            logger.error("Could not get local config - exiting")
            return False
        valid_config = True
        if local_config.get("input_folder", "") == "":
            valid_config = False
            logger.error(f"Could not find input folder in local config")
        if local_config.get("output_folder", "") == "":
            valid_config = False
            logger.error(f"Could not find output folder in local config")
        if not valid_config:
            logger.error("Invalid local configuration - exiting")
        return valid_config

    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        if self.s3_config is not None:
            return DataAccessS3(
                s3_credentials=self.s3_cred,
                s3_config=self.s3_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
            )
        elif self.lh_config is not None:
            return DataAccessLakeHouse(
                s3_credentials=self.s3_cred,
                lakehouse_config=self.lh_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
            )
        elif self.local_config is not None:
            return DataAccessLocal(
                path_config=self.local_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
            )
        else:
            logger.error("No data configuration is defined")
            return None
