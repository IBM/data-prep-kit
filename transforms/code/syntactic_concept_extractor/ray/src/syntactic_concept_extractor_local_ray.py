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
from data_processing.utils import ParamsUtils
from data_processing_ray.runtime.ray import RayTransformLauncher
from syntactic_concept_extractor_transform_ray import SyntacticConceptExtractorRayTransformConfiguration

# Define default input and output directories relative to the script's location
default_input_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "input"))
default_output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "../", "../", "output"))

# Use environment variables if provided, otherwise use the defaults
input_folder = os.getenv('INPUT_FOLDER', default_input_folder)
output_folder = os.getenv('OUTPUT_FOLDER', default_output_folder)

# Create local configuration
local_conf = {
    "input_folder": input_folder,
    "output_folder": output_folder,
    "contents": "Contents",
    "language": "Language"
}

# Code location and parameters
code_location = {"github": "github", "commit_hash": "12345", "path": "path"}
params = {
    "data_local_config": ParamsUtils.convert_to_ast(local_conf),
    "runtime_pipeline_id": "pipeline_id",
    "runtime_job_id": "job_id",
    "runtime_code_location": ParamsUtils.convert_to_ast(code_location),
}

if __name__ == "__main__":
    # Set the simulated command line args
    sys.argv = ParamsUtils.dict_to_req(d=params)
    print("In syntactic_concept_extractor_local_ray", sys.argv)
    # create launcher
    launcher = RayTransformLauncher(runtime_config=SyntacticConceptExtractorRayTransformConfiguration())
    print("In syntactic_concept_extractor_local_ray: Launcher created")

    # Launch the ray actor(s) to process the input
    launcher.launch()
