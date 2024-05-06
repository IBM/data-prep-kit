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
import ast
from typing import Union

from data_processing.data_access import (
    DataAccess,
    DataAccessFactoryBase,
    DataAccessLocal,
    DataAccessS3,
)
from data_processing.utils import ParamsUtils, str2bool


class DataAccessFactory(DataAccessFactoryBase):
    """
    This class is accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self, cli_arg_prefix: str = "data_", enable_data_navigation: bool = True):
        """
        Create the factory to parse a set of args that will then define the type of DataAccess object
        to be created by the create_data_access() method.
        :param cli_arg_prefix:  if provided, this will be prepended to all the CLI arguments names.
               Make sure it ends with _
        :param enable_data_navigation: if true enables CLI args and configuration for input/output paths,
            data sets, checkpointing, files to use, sampling and max files.
        This allows the creation of transform-specific (or other) DataAccess instances based on the
        transform-specific prefix (e.g. bl_ for blocklist transform).  The resulting keys returned
        in get_input_params() will include the prefix.  The underlying AST or other values of those
        keys is not effected by the prefix.
        """
        super().__init__(cli_arg_prefix=cli_arg_prefix)
        self.s3_config = None
        self.local_config = None
        self.enable_data_navigation = enable_data_navigation

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
            "access_key": ["access", "access key help text"],
            "secret_key": ["secret", "secret key help text"],
            "url": ["https://s3.us-east.cloud-object-storage.appdomain.cloud", "optional s3 url"],
            "region": ["us-east-1", "optional s3 region"],
        }
        parser.add_argument(
            f"--{self.cli_arg_prefix}s3_cred",
            type=ast.literal_eval,
            default=None,
            help="AST string of options for s3 credentials. Only required for S3 data access.\n"
            + ParamsUtils.get_ast_help_text(help_example_dict),
        )

        if self.enable_data_navigation:
            self.__add_data_navigation_params(parser)

    def __add_data_navigation_params(self, parser):
        help_example_dict = {
            "input_folder": [
                "s3-path/your-input-bucket",
                "Path to input folder of files to be processed",
            ],
            "output_folder": [
                "s3-path/your-output-bucket",
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
            f"--{self.cli_arg_prefix}data_sets",
            type=ast.literal_eval,
            default=None,
            help="List of sub-directories of input directory to use for input. For example, ['dir1', 'dir2']",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}files_to_use",
            type=ast.literal_eval,
            default=ast.literal_eval("['.parquet']"),
            help="list of file extensions to choose for input.",
        )
        parser.add_argument(
            f"--{self.cli_arg_prefix}num_samples", type=int, default=-1, help="number of random input files to process"
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
        local_config = arg_dict.get(f"{self.cli_arg_prefix}local_config")
        checkpointing = arg_dict.get(f"{self.cli_arg_prefix}checkpointing")
        max_files = arg_dict.get(f"{self.cli_arg_prefix}max_files", -1)
        data_sets = arg_dict.get(f"{self.cli_arg_prefix}data_sets")
        n_samples = arg_dict.get(f"{self.cli_arg_prefix}num_samples", -1)
        files_to_use = arg_dict.get(f"{self.cli_arg_prefix}files_to_use")
        # check which configuration (S3, LakeHouse, or Local) is specified
        s3_config_specified = 1 if s3_config is not None else 0
        local_config_specified = 1 if local_config is not None else 0

        # check that only one (S3, LakeHouse, or Local) configuration is specified
        if s3_config_specified + local_config_specified > 1:
            self.logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"{'S3, ' if s3_config_specified == 1 else ''}"
                f"{'Local ' if local_config_specified == 1 else ''}"
                "configurations specified, but only one configuration expected"
            )
            return False

        # further validate the specified configuration (S3, LakeHouse, or Local)
        if s3_config_specified == 1:
            if not self._validate_s3_config(s3_config=s3_config):
                return False
            self.s3_cred = s3_cred
            # S3 config requires S3 credentials
            if not self._validate_s3_cred(s3_credentials=self.s3_cred):
                return False
            self.s3_config = s3_config
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using S3 data access"
                f'input path - {self.s3_config["input_folder"]}, '
                f'output path - {self.s3_config["output_folder"]}'
            )
        elif local_config_specified == 1:
            if not self._validate_local_config(local_config=local_config):
                return False
            self.local_config = local_config
            self.logger.info(
                f"data factory {self.cli_arg_prefix} is using local data access"
                f"input_folder - {self.local_config['input_folder']} "
                f"output_folder - {self.local_config['output_folder']}"
            )
        elif s3_cred is not None:
            if not self._validate_s3_cred(s3_credentials=s3_cred):
                return False
            self.s3_cred = s3_cred
            self.logger.info(f"data factory {self.cli_arg_prefix} is using s3 configuration without input/output path")
        else:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} " f"is using local configuration without input/output path"
            )

        # Check whether both max_files and number samples are defined
        self.logger.info(f"data factory {self.cli_arg_prefix} max_files {max_files}, n_sample {n_samples}")
        if max_files > 0 and n_samples > 0:
            self.logger.error(
                f"data factory {self.cli_arg_prefix} "
                f"Both max files {max_files} and random samples {n_samples} are defined. Only one allowed at a time"
            )
            return False
        self.checkpointing = checkpointing
        self.max_files = max_files
        self.n_samples = n_samples
        self.files_to_use = files_to_use
        self.dsets = data_sets
        if data_sets is None or len(data_sets) < 1:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Not using data sets, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}"
            )
        else:
            self.logger.info(
                f"data factory {self.cli_arg_prefix} "
                f"Using data sets {self.dsets}, checkpointing {self.checkpointing}, max files {self.max_files}, "
                f"random samples {self.n_samples}, files to use {self.files_to_use}"
            )
        return True

    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        if self.s3_config is not None or self.s3_cred is not None:
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
