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
from data_processing.test_support import get_tables_in_folder
from data_processing.test_support.transform.table_transform_test import (
    AbstractTableTransformTest,
)
from hap_transform import HAPTransformConfiguration, HAPTransform

hap_params = {
    "model_name_or_path": 'ibm-granite/granite-guardian-hap-38m',
    "annotation_column": "hap_score",
    "doc_text_column": "contents",
    "inference_engine": "CPU",
    "max_length": 512,
    "batch_size": 128,
}

class TestHAPTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """
    def get_test_transform_fixtures(self) -> list[tuple]:
        src_file_dir = os.path.abspath(os.path.dirname(__file__))
        input_dir = os.path.join(src_file_dir, "../test-data/input")
        expected_dir = os.path.join(src_file_dir, "../test-data/expected")
        input_tables = get_tables_in_folder(input_dir)
        expected_tables = get_tables_in_folder(expected_dir)
    
        expected_metadata_list = [{}, {}]

	    # Transform the table
	    #table_list, metadata = transform.transform(table)
        
        fixtures = [
            (HAPTransform(hap_params), input_tables, expected_tables, expected_metadata_list),
        ]
        return fixtures