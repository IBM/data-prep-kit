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

import json
import os
import re
import sys
from typing import Any

from data_processing.utils import get_logger


logger = get_logger(__name__)


class KFPUtils:
    """
    Helper utilities for KFP implementations
    """

    @staticmethod
    def credentials(
        access_key: str = "S3_KEY", secret_key: str = "S3_SECRET", endpoint: str = "ENDPOINT"
    ) -> tuple[str, str, str]:
        """
        Get credentials from the environment
        :param access_key: environment variable for access key
        :param secret_key: environment variable for secret key
        :param endpoint: environment variable for S3 endpoint
        :return:
        """
        s3_key = os.getenv(access_key, None)
        s3_secret = os.getenv(secret_key, None)
        s3_endpoint = os.getenv(endpoint, None)
        if s3_key is None or s3_secret is None or s3_endpoint is None:
            logger.warning("Failed to load s3 credentials")
        return s3_key, s3_secret, s3_endpoint

    @staticmethod
    def get_namespace() -> str:
        """
        Get k8 namespace that we are running it
        :return:
        """
        ns = ""
        try:
            file = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r")
        except Exception as e:
            logger.warning(
                f"Failed to open /var/run/secrets/kubernetes.io/serviceaccount/namespace file, " f"exception {e}"
            )
        else:
            with file:
                ns = file.read()
        return ns

    @staticmethod
    def runtime_name(ray_name: str = "", run_id: str = "") -> str:
        """
        Get unique runtime name
        :param ray_name:
        :param run_id:
        :return: runtime name
        """
        # K8s objects cannot contain special characters, except '_', All characters should be in lower case.
        if ray_name != "":
            ray_name = ray_name.replace("_", "-").lower()
            pattern = r"[^a-zA-Z0-9-]"  # the ray_name cannot contain upper case here, but leave it just in case.
            ray_name = re.sub(pattern, "", ray_name)
        else:
            ray_name = "a"
        # the return value plus namespace name will be the name of the Ray Route,
        # which length is restricted to 64 characters,
        # therefore we restrict the return name by 15 character.
        if run_id != "":
            return f"{ray_name[:9]}-{run_id[:5]}"
        return ray_name[:15]

    @staticmethod
    def dict_to_req(d: dict[str, Any], executor: str = "transformer_launcher.py") -> str:
        res = f"python {executor} "
        for key, value in d.items():
            if isinstance(value, str):
                res += f'--{key}="{value}" '
            else:
                res += f"--{key}={value} "
        return res

    # Load a string that represents a json to python dictionary
    @staticmethod
    def load_from_json(js: str) -> dict[str, Any]:
        try:
            return json.loads(js)
        except Exception as e:
            logger.warning(f"Failed to load parameters {js} with error {e}")
            sys.exit(1)
