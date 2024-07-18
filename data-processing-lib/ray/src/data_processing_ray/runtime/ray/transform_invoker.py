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
import os
from typing import Any
from data_processing.utils import ParamsUtils, get_logger
from data_processing_ray.runtime.ray import RayTransformLauncher
from data_processing.utils import PipInstaller, TransformRuntime, TransformsConfiguration, import_class

project = "https://github.com/IBM/data-prep-kit.git"
logger = get_logger(__name__)


def execute_ray_transform(configuration: TransformsConfiguration, name: str,
                             input_folder: str, output_folder: str, params: dict[str, Any]) -> bool:
    # get transform configuration
    r_subdirectory, r_l_name, extra_libraries, t_class = (configuration.
                                     get_configuration(transform=name, runtime=TransformRuntime.RAY))
    if r_subdirectory is None:
        return False
    p_subdirectory, p_l_name, _, _ = (configuration.get_configuration(transform=name, runtime=TransformRuntime.PYTHON))

    installer = PipInstaller()
    # Ray installer can depend on Python installer, if this is the case install Python one first
    p_installed = False
    if p_subdirectory is not None and not installer.validate(name=r_l_name):
        if not installer.install(project=project, subdirectory=p_subdirectory, name=p_l_name):
            logger.warning(f"failed to install transform {name}")
            return False
        p_installed = True

    # Check if transformer already installed
    r_installed = False
    if not installer.validate(name=r_l_name):
        # transformer is not installed, install it
        if not installer.install(project=project, subdirectory=r_subdirectory, name=r_l_name):
            logger.warning(f"failed to install transform {name}")
            return False
        r_installed = True
    # configure input parameters
    location = {
        "data_local_config": ParamsUtils.convert_to_ast({
            "input_folder": input_folder,
            "output_folder": output_folder,
        })}
    p = location | params | {"run_locally": True}
    # create configuration
    klass = import_class(t_class)
    transform_configuration = klass()
    # Set the command line args
    current_args = sys.argv
    sys.argv = ParamsUtils.dict_to_req(d=p)
    # create launcher
    launcher = RayTransformLauncher(runtime_config=transform_configuration)
    # Launch the ray actor(s) to process the input
    res = launcher.launch()
    # restore args
    sys.argv = current_args
    # clean up
    if p_installed:
        # we installed transformer, uninstall it
        if not installer.uninstall(name=p_l_name):
            logger.warning(f"failed uninstall transform {r_l_name}")
    if r_installed:
        # we installed transformer, uninstall it
        if not installer.uninstall(name=r_l_name):
            logger.warning(f"failed uninstall transform {r_l_name}")
        # uninstall support libraries
        for library in extra_libraries:
            if not installer.uninstall(name=library):
                logger.warning(f"failed uninstall transform {library}")
    if res == 0:
        return True
    logger.warning(f"failed execution of transform {name}")
    return False