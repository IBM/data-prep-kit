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

import warnings


warnings.filterwarnings("ignore")

import subprocess
import sys

import pkg_resources


class PipInstaller:
    """
    Implements programmatic installation/uninstallation of the library.
    """

    @staticmethod
    def install(project: str, subdirectory: str, name: str) -> bool:
        """
        Install library from GIT
        :param project: git project
        :param subdirectory: sub directory
        :param name: name
        :return: True if installation is successful, False in the case of failures
        """
        # build pip package string
        package = f"git+{project}#subdirectory={subdirectory}&egg={name}"
        try:
            # Do pip install (in sub process)
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            return True
        except Exception as e:
            # process exception
            print(f"Exception installing package {name}: {e}")
            return False

    @staticmethod
    def validate(name: str) -> bool:
        """
        Check
        :param name: name
        :return: True if the library is present, False otherwise
        """
        try:
            pkg_resources.get_distribution(name)
            return True
        except pkg_resources.DistributionNotFound:
            return False

    @staticmethod
    def uninstall(name: str):
        """
        Uninstall library
        :param name: name
        :return: True if the un installation succeeds, False otherwise
        """
        try:
            # Invoke pip (in subprocess) to uninstall
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", name])
            return True
        except Exception as e:
            # process exception
            print(f"Exception uninstalling package {name}: {e}")
            return False
