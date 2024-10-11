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


import json
from argparse import ArgumentParser, Namespace

import pyarrow as pa
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import (
    CLIArgumentProvider,
    TransformUtils,
    get_logger,
    str2bool,
)
from transformer import AllowLicenseStatusTransformer, DenyLicenseStatusTransformer


logger = get_logger(__name__)

LICENSE_SELECT_PARAMS = "license_select_params"

shortname = "lc"
CLI_PREFIX = f"{shortname}_"

LICENSE_COLUMN_NAME_KEY = "license_column_name"
LICENSE_COLUMN_NAME_CLI_KEY = f"{CLI_PREFIX}{LICENSE_COLUMN_NAME_KEY}"

DENY_LICENSES_KEY = "deny_licenses"
DENY_LICENSES_CLI_KEY = f"{CLI_PREFIX}{DENY_LICENSES_KEY}"

LICENSES_FILE_KEY = "licenses_file"
LICENSES_FILE_CLI_KEY = f"{CLI_PREFIX}{LICENSES_FILE_KEY}"

ALLOW_NO_LICENSE_KEY = "allow_no_license"
ALLOW_NO_LICENSE_CLI_KEY = f"{CLI_PREFIX}{ALLOW_NO_LICENSE_KEY}"

LICENSE_COLUMN_DEFAULT = "license"
LICENSES_KEY = "licenses"

def _get_supported_licenses(license_file: str, data_access: DataAccess) -> list[str]:
    logger.info(f"Getting supported licenses from file {license_file}")
    licenses_list = None
    try:
        licenses_list_json, _ = data_access.get_file(license_file)
        licenses_list = json.loads(licenses_list_json.decode("utf-8"))
        logger.info(f"Read a list of {len(licenses_list)} licenses.")
    except Exception as e:
        logger.error(f"Failed to read file: {license_file} due to {e}")
    return licenses_list


class LicenseSelectTransform(AbstractTableTransform):
    """It can be used to select the rows/records of data with licenses
    matching those in the approved/deny list. It adds a new column: `license_status`
    to indicate the selected/denied licenses.

       config: dictionary of configuration data
                license_select_params: A dictionary with the following keys.
                    license_column_name - The name of the column with license, default: 'licence'.
                    allow_no_license - Allows to select rows with no license. default: False
                    licenses - A list of licenses
                    deny_licenses - if selected, the the licenses list is used as deny list, default: False
        Example:
                config = {
                    "license_select_params": {
                        "license_column_name": "license",
                        "allow_no_license": False,
                        "licenses": ["MIT", "Apache 2.0"],
                        "deny_licenses": False
                        }
                }
    """

    def __init__(self, config: dict):
        super().__init__(config)

        try:
            self.license_select = config.get(LICENSE_SELECT_PARAMS)
            self.license_column = self.license_select.get(LICENSE_COLUMN_NAME_KEY, LICENSE_COLUMN_DEFAULT)
            allow_no_license = self.license_select.get(ALLOW_NO_LICENSE_KEY, False)
            licenses = self.license_select.get(LICENSES_KEY, None)
            if not licenses or not isinstance(licenses, list):
                raise ValueError("license list not found.")
            deny = self.license_select.get(DENY_LICENSES_KEY, False)
            logger.debug(f"LICENSE_SELECT_PARAMS: {self.license_select}")
        except Exception as e:
            raise ValueError(f"Invalid Argument: cannot create LicenseSelectTransform object: {e}.")

        if not deny:
            self.transformer = AllowLicenseStatusTransformer(
                license_column=self.license_column,
                allow_no_license=allow_no_license,
                licenses=licenses,
            )
        else:
            self.transformer = DenyLicenseStatusTransformer(
                license_column=self.license_column,
                allow_no_license=allow_no_license,
                licenses=licenses,
            )

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict]:
        """
        Transforms input tables by adding a boolean `license_status` column
        indicating whether the license is approved/denied.
        """
        TransformUtils.validate_columns(table=table, required=[self.license_column])
        new_table = self.transformer.transform(table)
        return [new_table], {}

class LicenseSelectTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(name="license_select", transform_class=LicenseSelectTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{LICENSE_COLUMN_NAME_CLI_KEY}",
            required=False,
            type=str,
            default=LICENSE_COLUMN_DEFAULT,
            help="Name of the column holds the data to process",
        )
        parser.add_argument(
            f"--{ALLOW_NO_LICENSE_CLI_KEY}",
            required=False,
            type=lambda x: bool(str2bool(x)),
            default=False,
            help="allow entries with no associated license (default: false)",
        )
        parser.add_argument(
            f"--{LICENSES_FILE_CLI_KEY}",
            required=True,
            type=str,
            help="S3 or local path to allowed/denied licenses JSON file",
        )
        parser.add_argument(
            f"--{DENY_LICENSES_CLI_KEY}",
            type=lambda x: bool(str2bool(x)),
            required=False,
            default=False,
            help="allow all licences except those in licenses_file (default: false)",
        )
        # Create the DataAccessFactor to use CLI args
        self.daf = DataAccessFactory(CLI_PREFIX, False)
        # Add the DataAccessFactory parameters to the transform's configuration parameters.
        self.daf.add_input_params(parser)

    def apply_input_params(self, args: Namespace) -> bool:
        if not self.daf.apply_input_params(args):
            return False
        
        captured = CLIArgumentProvider.capture_parameters(args, CLI_PREFIX, False)
        license_column_name = captured.get(LICENSE_COLUMN_NAME_KEY)
        allow_licenses = captured.get(ALLOW_NO_LICENSE_KEY)
        deny_licenses = captured.get(DENY_LICENSES_KEY, False)
        licenses_file = captured.get(LICENSES_FILE_KEY)
        
        # Read licenses from allow-list or deny-list
        data_access = self.daf.create_data_access()
        licenses = _get_supported_licenses(licenses_file, data_access)

        self.params = {
            LICENSE_SELECT_PARAMS: {
                LICENSE_COLUMN_NAME_KEY: license_column_name,
                ALLOW_NO_LICENSE_KEY: allow_licenses,
                DENY_LICENSES_KEY: deny_licenses,
                LICENSES_KEY: licenses,
            }
        }
        return True
