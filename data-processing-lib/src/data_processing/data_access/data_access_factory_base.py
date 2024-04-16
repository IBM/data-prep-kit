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
import uuid
from typing import Any, Union

from data_processing.data_access import DataAccess
from data_processing.utils import CLIArgumentProvider, get_logger


class DataAccessFactoryBase(CLIArgumentProvider):
    """
    This is a base class for accepting Data Access parameters, validates them and instantiates an appropriate
    Data Access class based on these parameters.
    This class has to be serializable, so that we can pass it to the actors
    """

    def __init__(self, cli_arg_prefix: str = "data_"):
        """
        Create the factory to parse a set of args that will then define the type of DataAccess object
        to be created by the create_data_access() method.
        :param cli_arg_prefix:  if provided, this will be prepended to all the CLI arguments names.
               Make sure it ends with _
        """
        self.s3_cred = None
        self.checkpointing = False
        self.dsets = None
        self.max_files = -1
        self.n_samples = -1
        self.files_to_use = []
        self.cli_arg_prefix = cli_arg_prefix
        self.params = {}
        self.logger = get_logger(__name__ + str(uuid.uuid4()))

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
        pass

    def apply_input_params(self, args: Union[dict, argparse.Namespace]) -> bool:
        """
        Validate data access specific parameters
        This might need to be extended if new data access implementation is added
        :param args: user defined arguments
        :return: None
        """
        pass

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

    def create_data_access(self) -> DataAccess:
        """
        Create data access based on the parameters
        :return: corresponding data access class
        """
        pass

    """
    Some commonly useful validation methods
    """

    def _validate_s3_cred(self, s3_credentials: dict[str, str]) -> bool:
        """
        Validate that
        :param s3_credentials: dictionary of S3 credentials
        :return:
        """
        if s3_credentials is None:
            self.logger.error(f"data access factory {self.cli_arg_prefix}: missing s3_credentials")
            return False
        valid_config = True
        if s3_credentials.get("access_key") is None:
            self.logger.error(f"data access factory {self.cli_arg_prefix}: missing S3 access_key")
            valid_config = False
        if s3_credentials.get("secret_key") is None:
            self.logger.error(f"data access factory {self.cli_arg_prefix}: missing S3 secret_key")
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
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: " "Could not find input folder in local config"
            )
        if local_config.get("output_folder", "") == "":
            valid_config = False
            self.logger.error(
                f"data access factory {self.cli_arg_prefix}: " "Could not find output folder in local config"
            )
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
            self.logger.error(f"data access factory {self.cli_arg_prefix}: Could not find input folder in s3 config")
        if s3_config.get("output_folder", "") == "":
            valid_config = False
            self.logger.error(f"data access factory {self.cli_arg_prefix}: Could not find output folder in s3 config")
        return valid_config
