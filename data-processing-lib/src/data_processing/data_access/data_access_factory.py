import argparse
import ast
from typing import Any, Union

from data_processing import utils
from data_processing.data_access import (
    DataAccess,
    DataAccessLakeHouse,
    DataAccessLocal,
    DataAccessS3,
)
from data_processing.utils import (
    CLIArgumentProvider,
    DPFConfig,
    ParamsUtils,
    get_logger,
    str2bool,
)


logger = get_logger(__name__)


class DataAccessFactory(CLIArgumentProvider):
    """
    This class is accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self, cli_arg_prefix: str = "data_"):
        """
        Create the factory to parse a set of args that will then define the type of DataAccess object
        to be created by the create_data_access() method.
        :param cli_arg_prefix:  if provided, this will be prepended to all the CLI arguments names.
               Make sure it ends with _
        This allows the creation of transform-specific (or other) DataAccess instances based on the
        transform-specific prefix (e.g. bl_ for blocklist transform).  The resulting keys returned
        in get_input_params() will include the prefix.  The underlying AST or other values of those
        keys is not effected by the prefix.
        """
        self.s3_config = None
        self.lh_config = None
        self.local_config = None
        self.s3_cred = None
        self.checkpointing = False
        self.dsets = None
        self.max_files = -1
        self.n_samples = -1
        self.files_to_use = []
        self.cli_arg_prefix = cli_arg_prefix
        self.params = {}

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
            "url": ["s3:/cos-optimal-llm-pile/test/", "S3 url"],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_cred",
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
                "Path to output folder of processed files",
            ],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_config",
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
            f"--{self.cli_arg_prefix}lh_config",
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
            f"--{self.cli_arg_prefix}local_config",
            type=ast.literal_eval,
            default=None,
            help="ast string containing input/output folders using local fs.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}max_files", type=int, default=-1, help="Max amount of files to process"
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}checkpointing",
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="checkpointing flag",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}data_sets", type=str, default=None, help="List of data sets")
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_use",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of files extensions to choose",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}num_samples",
            type=int,
            default=-1,
            help="number of random files to process"
        )

    def apply_input_params(self, args: Union[dict, argparse.Namespace]) -> bool:
        """
        Validate data access specific parameters
        This might need to be extended if new data access implementation is added
        :param args: user defined arguments
        :return: None
        """
        if isinstance(args, argparse.Namespace):
            arg_dict = vars(args)
        elif isinstance(args, dict):
            arg_dict = args
        else:
            raise ValueError("args must be Namespace or dictionary")
        s3_cred = arg_dict.get(f"{self.cli_arg_prefix}s3_cred")
        s3_config = arg_dict.get(f"{self.cli_arg_prefix}s3_config")
        lh_config = arg_dict.get(f"{self.cli_arg_prefix}lh_config")
        local_config = arg_dict.get(f"{self.cli_arg_prefix}local_config")
        checkpointing = arg_dict.get(f"{self.cli_arg_prefix}checkpointing")
        max_files = arg_dict.get(f"{self.cli_arg_prefix}max_files")
        data_sets = arg_dict.get(f"{self.cli_arg_prefix}data_sets")
        n_samples = arg_dict.get(f"{self.cli_arg_prefix}num_samples")
        files_to_use = arg_dict.get(f"{self.cli_arg_prefix}files_to_use")
        # check which configuration (S3, LakeHouse, or Local) is specified
        s3_config_specified = 1 if s3_config is not None else 0
        lh_config_specified = 1 if lh_config is not None else 0
        local_config_specified = 1 if local_config is not None else 0

        # check that only one (S3, LakeHouse, or Local) configuration is specified
        if s3_config_specified + lh_config_specified + local_config_specified > 1:
            logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"{'S3, ' if s3_config_specified == 1 else ''}"
                f"{'Lakehouse, ' if lh_config_specified == 1 else ''}"
                f"{'Local ' if local_config_specified == 1 else ''}"
                "configurations specified, but only one configuration expected"
            )
            return False

        # further validate the specified configuration (S3, LakeHouse, or Local)
        if s3_config_specified == 1:
            if not self._validate_s3_config(s3_config=s3_config):
                return False
            self.s3_cred = s3_cred
            if not self.__validate_s3_cred(s3_credentials=self.s3_cred):
                return False
            self.s3_config = s3_config
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                f'Using s3 configuration: input path - {self.s3_config["input_folder"]}, '
                f'output path - {self.s3_config["output_folder"]}'
            )
        elif lh_config_specified == 1:
            if not self._validate_lh_config(lh_config=lh_config):
                return False
            self.s3_cred = s3_cred
            if not self.__validate_s3_cred(s3_credentials=self.s3_cred):
                return False
            self.lh_config = lh_config
            utils.add_if_missing(self.lh_config, "token", DPFConfig.LAKEHOUSE_TOKEN)
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                f'Using lake house configuration: input table - {self.lh_config["input_table"]}, '
                f'input_dataset - {self.lh_config["input_dataset"]}, '
                f'input_version - {self.lh_config["input_version"]}, '
                f'output table - {self.lh_config["output_table"]}, '
                f'output_path - {self.lh_config["output_path"]}, '
                f'lh_environment - {self.lh_config["lh_environment"]} '
            )
        elif local_config_specified == 1:
            if not self._validate_local_config(local_config=local_config):
                return False
            self.local_config = local_config
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Using local configuration with: "
                f"input_folder - {self.local_config['input_folder']} "
                f"output_folder - {self.local_config['output_folder']}"
            )
        elif s3_cred is not None:
            if not self.__validate_s3_cred(s3_credentials=self.s3_cred):
                return False
            self.s3_cred = s3_cred
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                'Using s3 configuration without input/output path'
            )
        else:
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                'Using local configuration without input/output path'
            )

        # Check whether both max_files and number samples are defined
        if max_files > 0 and n_samples > 0:
            logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"Both max files {max_files} and random samples {n_samples} are defined. Only one allowed at a time")
            return False
        self.checkpointing = checkpointing
        self.max_files = max_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        if data_sets is None or len(data_sets) < 1:
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Not using data sets, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}")
        else:
            self.dsets = data_sets.split(",")
            logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Using data sets {self.dsets}, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}"
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
            "random_samples": self.n_samples,
            "files_to_use": self.files_to_use,
        }
        if self.dsets is not None:
            params["data sets"] = self.dsets
        return params

    def __validate_s3_cred(self, s3_credentials: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_credentials: dictionary of S3 credentials
        :return:
        """
        if s3_credentials is None:
            logger.error(f"prefix '{self.cli_arg_prefix}': missing s3_credentials")
            return False
        valid_config = True
        if s3_credentials.get("access_key") is None:
            logger.error(f"prefix '{self.cli_arg_prefix}': missing S3 access_key")
            valid_config = False
        if s3_credentials.get("secret_key") is None:
            logger.error(f"prefix '{self.cli_arg_prefix}': missing S3 secret_key")
            valid_config = False
        if s3_credentials.get("url") is None:
            logger.error(f"prefix '{self.cli_arg_prefix}': missing S3 url")
            valid_config = False
        return valid_config

    def _validate_local_config(self, local_config: dict[str, str]) -> bool:
        """
        Validate that
        :param local_config: dictionary of local config
        :return: True if local config is valid, False otherwise
        """
        valid_config = True
        if local_config.get("input_folder", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find input folder in local config")
        if local_config.get("output_folder", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find output folder in local config")
        return valid_config

    def _validate_s3_config(self, s3_config: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_config: dictionary of local config
        :return: True if s3l config is valid, False otherwise
        """
        valid_config = True
        if s3_config.get("input_folder", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find input folder in s3 config")
        if s3_config.get("output_folder", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find output folder in s3 config")
        return valid_config

    def _validate_lh_config(self, lh_config: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_config: dictionary of local config
        :return: True if s3l config is valid, False otherwise
        """
        valid_config = True
        if lh_config.get("input_table", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find input table in lh config")
        if lh_config.get("input_dataset", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find input_dataset in lh config")
        if lh_config.get("input_version", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find input_version in lh config")
        if lh_config.get("output_table", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find output_table in lh config")
        if lh_config.get("output_path", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find output_path in lh config")
        if lh_config.get("lh_environment", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find lh_environment in lh config")
        if lh_config.get("token", "") == "":
            valid_config = False
            logger.error(f"prefix '{self.cli_arg_prefix}': Could not find lh token in lh config")
        return valid_config

    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        if self.lh_config is not None:
            # LH data access is only for orchestrator data access, so it should always be present
            return DataAccessLakeHouse(
                s3_credentials=self.s3_cred,
                lakehouse_config=self.lh_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
            )
        elif self.s3_config is not None or self.s3_cred is not None:
            # If S3 config or S3 credential are specified, its S3
            return DataAccessS3(
                s3_credentials=self.s3_cred,
                s3_config=self.s3_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
            )
        else:
            # anything else is local data
            return DataAccessLocal(
                path_config=self.local_config,
                d_sets=self.dsets,
                checkpoint=self.checkpointing,
                m_files=self.max_files,
                n_samples=self.n_samples,
                files_to_use=self.files_to_use,
            )
