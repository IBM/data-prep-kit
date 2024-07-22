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
import boto3
import moto
from moto.server import ThreadedMotoServer

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from data_processing.utils import ParamsUtils
from doc_quality_transform_python import DocQualityPythonTransformConfiguration

s3_port = 8085
server = ThreadedMotoServer(port=s3_port)
server.start()

bucket_name = 'testbucket'
bad_word_path = 'ldnoobw/en'
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

@moto.mock_aws
class TestPythonDocQualityTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    client = boto3.client('s3',
                            aws_access_key_id="access",
                            aws_secret_access_key="secret",
                            endpoint_url=f"http://localhost:{s3_port}",
                            region_name='us-east-1')
    client.create_bucket(Bucket=bucket_name)
    local_bad_word_path = os.path.join(basedir, "ldnoobw", "en")
    client.upload_file(local_bad_word_path, bucket_name, bad_word_path)
    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        cli_params = {
            "docq_text_lang": "en",
            "docq_doc_content_column": "contents",
            "docq_bad_word_filepath": os.path.join(bucket_name, bad_word_path),
            "docq_s3_cred": ParamsUtils.convert_to_ast({
                "access_key": "access",
                "secret_key": "secret",
                "url": f"http://localhost:{s3_port}",
                "region": "us-east-1",
            }),
        }
        basedir = os.path.abspath(os.path.join(basedir, "test-data"))
        fixtures = []
        launcher = PythonTransformLauncher(DocQualityPythonTransformConfiguration())
        fixtures.append((launcher, cli_params, os.path.join(basedir, "input"), os.path.join(basedir, "expected")))
        return fixtures
