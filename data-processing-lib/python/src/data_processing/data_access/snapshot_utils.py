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

from data_processing.data_access import DataAccess


class SnapshotUtils:
    """
    Class implementing support methods for snapshotting
    """

    @staticmethod
    def get_snapshot_folder(data_access: DataAccess) -> str:
        """
        Get snapshot folder from data access
        :param data_access: data access class
        :return: output folder
        """
        output_folder = data_access.get_output_folder()
        if not output_folder.endswith("/"):
            output_folder += "/"
        return f"{output_folder}snapshot/"
