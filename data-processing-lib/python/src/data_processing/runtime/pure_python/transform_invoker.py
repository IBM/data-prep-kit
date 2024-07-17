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

import sys
from typing import Any
from data_processing.utils import ParamsUtils, get_logger
from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.utils import PipInstaller, TransformRuntime, TransformsConfiguration, import_class

project = "https://github.com/IBM/data-prep-kit.git"
logger = get_logger(__name__)


def execute_python_transform(configuration: TransformsConfiguration, name: str,
                             input_folder: str, output_folder: str, params: dict[str, Any]) -> bool:
    # get transform configuration
    logger.info(f"available transforms: {configuration.get_available_transforms()}")
    subdirectory, l_name, extra_libraries, t_class = (configuration.
                                     get_configuration(transform=name, runtime=TransformRuntime.PYTHON))
    if subdirectory is None:
        return False

    installer = PipInstaller()
    # Check if transformer already installed
    installed = False
    if not installer.validate(name=l_name):
        # transformer is not installed, install it
        if not installer.install(project=project, subdirectory=subdirectory, name=l_name):
            logger.warning(f"failed to install transform {name}")
            return False
        installed = True
    # configure input parameters
    location = {
        "data_local_config": ParamsUtils.convert_to_ast({
            "input_folder": input_folder,
            "output_folder": output_folder,
        })}
    p = location | params
    # create configuration
    klass = import_class(t_class)
    transform_configuration = klass()
    # Set the command line args
    current_args = sys.argv
    sys.argv = ParamsUtils.dict_to_req(d=p)
    # create launcher
    launcher = PythonTransformLauncher(runtime_config=transform_configuration)
    # Launch the ray actor(s) to process the input
    res = launcher.launch()
    # restore args
    sys.argv = current_args
    if installed:
        # we installed transformer, uninstall it
        if not installer.uninstall(name=l_name):
            logger.warning(f"failed uninstall transform {l_name}")
        for library in extra_libraries:
            if not installer.uninstall(name=library):
                logger.warning(f"failed uninstall transform {library}")
    if res == 0:
        return True
    logger.warning(f"failed execution of transform {name}")
    return False
