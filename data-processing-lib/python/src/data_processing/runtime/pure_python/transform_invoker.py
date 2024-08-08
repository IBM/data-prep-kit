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

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.runtime.transform_launcher import AbstractTransformLauncher
from data_processing.utils import (
    ParamsUtils,
    PipInstaller,
    TransformRuntime,
    TransformsConfiguration,
    get_logger,
)


project = "https://github.com/IBM/data-prep-kit.git"
logger = get_logger(__name__)


def _import_class(name):
    """
    Import class by name
    :param name: name
    :return:
    """
    components = name.split(".")
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def invoke_transform(
    name: str,
    t_class: str,
    launcher: AbstractTransformLauncher,
    input_folder: str,
    output_folder: str,
    s3_config: dict[str, Any],
    params: dict[str, Any],
) -> bool:
    """
    Invoke transform
    :param name: transform name
    :param t_class: transform configuration class
    :param launcher: transform launcher
    :param input_folder: input folder (local or S3)
    :param output_folder: output folder (local or S3)
    :param s3_config: S3 configuration - None local data
    :param params: transform parameters
    :return:
    """
    # Create data access params
    if s3_config is None:
        logger.info("Using local data")
        data_params = {
            "data_local_config": ParamsUtils.convert_to_ast(
                {
                    "input_folder": input_folder,
                    "output_folder": output_folder,
                }
            )
        }
    else:
        logger.info("Using data from S3")
        data_params = {
            "data_s3_conf": ParamsUtils.convert_to_ast(
                {
                    "input_folder": input_folder,
                    "output_folder": output_folder,
                }
            ),
            "data_s3_config": ParamsUtils.convert_to_ast(s3_config),
        }
    # create configuration
    klass = _import_class(t_class)
    transform_configuration = klass()
    # Set the command line args
    current_args = sys.argv
    sys.argv = ParamsUtils.dict_to_req(d=data_params | params)
    # create launcher
    try:
        launcher = launcher(runtime_config=transform_configuration)
        # Launch the ray actor(s) to process the input
        res = launcher.launch()
    except (Exception, SystemExit) as e:
        logger.warning(f"Exception executing transform {name}: {e}")
        res = 1
    # restore args
    sys.argv = current_args
    return res


def execute_python_transform(
    configuration: TransformsConfiguration,
    name: str,
    params: dict[str, Any],
    input_folder: str,
    output_folder: str,
    s3_config: dict[str, Any] = None,
) -> bool:
    """
    Execute Python transform
    :param configuration: transforms configuration
    :param name: transform name
    :param params: transform params
    :param input_folder: input folder (local or S3)
    :param output_folder: output folder (local or S3)
    :param s3_config: S3 configuration - None local data
    :return: True/False - execution result
    """
    # get transform configuration
    subdirectory, l_name, extra_libraries, t_class = configuration.get_configuration(
        transform=name, runtime=TransformRuntime.PYTHON
    )
    if subdirectory is None:
        return False

    installer = PipInstaller()
    # Check if transformer already installed
    installed = False
    if not installer.validate(name=l_name):
        # transformer is already installed, skip it, otherwise, install it
        if not installer.install(project=project, subdirectory=subdirectory, name=l_name):
            logger.warning(f"failed to install transform {name}")
            return False
        installed = True
    # invoke transform
    res = invoke_transform(
        name=name,
        t_class=t_class,
        launcher=PythonTransformLauncher,
        input_folder=input_folder,
        output_folder=output_folder,
        s3_config=s3_config,
        params=params,
    )
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
