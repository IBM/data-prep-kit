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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.utils import ParamsUtils
from doc_quality_transform_python import DocQualityPythonTransformConfiguration
from perplexity import (
    get_model_file_name,
    get_tokenizer_file_name,
)

import boto3
import moto
from moto.server import ThreadedMotoServer

s3_port = 8085

server = ThreadedMotoServer(port=s3_port)
server.start()

bucket_name = 'testbucket'
model_path = 'lm_sp'
text_lang = 'en'
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

@moto.mock_aws
class TestPythonDocQualityTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    kenLM_model_path = os.path.join(basedir, "lm_sp")
    if not os.path.exists(kenLM_model_path):
        kenLM_model_path = os.path.abspath(os.path.join(basedir, "..", "lm_sp"))

    client = boto3.client('s3',
                            aws_access_key_id="access",
                            aws_secret_access_key="secret",
                            endpoint_url=f"http://localhost:{s3_port}",
                            region_name='us-east-1')
    client.create_bucket(Bucket=bucket_name)
    local_model_path = f"{kenLM_model_path}/{get_model_file_name(text_lang)}"
    s3_model_path = f"{model_path}/{get_model_file_name(text_lang)}"
    local_tokenizer_path = f"{kenLM_model_path}/{get_tokenizer_file_name(text_lang)}"
    s3_tokenizer_path = f"{model_path}/{get_tokenizer_file_name(text_lang)}"
    client.upload_file(local_model_path, bucket_name, s3_model_path)
    client.upload_file(local_tokenizer_path, bucket_name, s3_tokenizer_path)

    def get_test_transform_fixtures(self) -> list[tuple]:
        cli_params = {
            "docq_text_lang": "en",
            "docq_doc_content_column": "contents",
            "docq_bad_word_filepath": os.path.join(basedir, "ldnoobw", "en"),
            "docq_kenLM_model": os.path.join(bucket_name, model_path),
            "docq_s3_cred": ParamsUtils.convert_to_ast({
                "access_key": "access",
                "secret_key": "secret",
                "url": f"http://localhost:{s3_port}",
                "region": "us-east-1",
            }),
        }
        test_file_dir = os.path.abspath(os.path.join(basedir, "test-data"))
        fixtures = []
        launcher = PythonTransformLauncher(DocQualityPythonTransformConfiguration())
        fixtures.append((launcher, cli_params, os.path.join(test_file_dir, "input"), os.path.join(test_file_dir, "expected")))
        return fixtures
