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

import os
from typing import Any, Union


class DPLConfig:
    @staticmethod
    def _get_first_env_var(env_var_list: list[str]) -> Union[str, None]:
        for var in env_var_list:
            value = os.environ.get(var, None)
            if value is not None:
                # print(f"Found env var {var}", flush=True)
                return value
        # print(f"Did not find any of the following env vars {env_var_list}")
        return None

    S3_ACCESS_KEY = _get_first_env_var(["DPL_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "COS_ACCESS_KEY"])
    """ Set from DPL_S3_ACCESS_KEY, AWS_ACCESS_KEY_ID or COS_ACCESS_KEY env vars """
    S3_SECRET_KEY = _get_first_env_var(["DPL_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY", "COS_SECRET_KEY"])
    """ Set from DPL_S3_SECRET_KEY, AWS_SECRET_ACCESS_KEY or COS_SECRET_KEY env vars """
    LAKEHOUSE_TOKEN = _get_first_env_var(["DPL_LAKEHOUSE_TOKEN", "LAKEHOUSE_TOKEN"])
    """ Set from DPL_LAKEHOUSE_TOKEN or LAKEHOUSE_TOKEN env vars """
    HUGGING_FACE_TOKEN = _get_first_env_var(["DPL_HUGGING_FACE_TOKEN"])
    """ Set from DPL_HUGGING_FACE_TOKEN env var(s) """

    DEFAULT_LOG_LEVEL = os.environ.get("DPL_LOG_LEVEL", "INFO")


def add_if_missing(config: dict[str, Any], key: str, dflt: Any):
    """
    Add the given default key value if there no value for the key in the dictionary.
    :param config:
    :param key:
    :param dflt:
    :return:
    """
    if config is None:
        return
    value = config.get(key)
    if value is None:
        config[key] = dflt
