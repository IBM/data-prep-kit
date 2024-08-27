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
from typing import Tuple

from data_processing.test_support import get_tables_in_folder
from data_processing.data_access import DataAccessFactory
from data_processing.test_support.transform import AbstractTableTransformTest
from fdedup.utils import DocCollector
from fdedup.transforms.base import (doc_column_name_key,
                                    int_column_name_key,
                                    cluster_column_name_key,
                                    removed_docs_column_name_key,
                                    doc_id_cache_key,
                                    )
from fdedup.transforms.python import FdedupFilterTransform


class TestFdedupPreprocessorTransform(AbstractTableTransformTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[Tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test-data"))
        input_dir = os.path.join(basedir, "input")
        input_tables = get_tables_in_folder(input_dir)
        data_access_factory = DataAccessFactory()
        id_file = os.path.join(basedir, "input/snapshot/docs/doc_collector_0")
        doc_collector = DocCollector({"id": 0, "data_access": data_access_factory, "snapshot": id_file})
        fdedup_params = {doc_column_name_key: "contents", int_column_name_key: "Unnamed: 0",
                         cluster_column_name_key: "cluster", removed_docs_column_name_key: "removed",
                         doc_id_cache_key: doc_collector}
        expected_metadata_list = [{'source_documents': 5, 'result_documents': 3}, {}]
        expected_tables = get_tables_in_folder(os.path.join(basedir, "filter"))
        return [
            (FdedupFilterTransform(fdedup_params), input_tables, expected_tables, expected_metadata_list),
        ]
