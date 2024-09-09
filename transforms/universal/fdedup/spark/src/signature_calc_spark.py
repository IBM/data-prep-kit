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
import sys

import polars as pl
from data_processing.utils import ParamsUtils
from data_processing_spark.runtime.spark import SparkTransformLauncher
from signature_calc_transform_spark import (
    SignatureCalculationSparkTransformConfiguration,
)


if __name__ == "__main__":
    sys.argv.append("--data_s3_cred")
    s3_creds = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "url": os.getenv("AWS_ENDPOINT_URL"),
    }
    sys.argv.append(ParamsUtils.convert_to_ast(s3_creds))
    # create launcher
    launcher = SparkTransformLauncher(runtime_config=SignatureCalculationSparkTransformConfiguration())
    # Launch the spark worker(s) to process the input
    launcher.launch()
