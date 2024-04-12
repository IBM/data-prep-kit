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

from kfp_support.workflow_support.utils import PipelinesUtils


def test_pipelines():
    """
    Test pipelines utils
    """
    utils = PipelinesUtils(host="http://localhost:8080/kfp")
    # get pipeline by name
    pipeline = utils.get_pipeline_by_name("[Tutorial] Data passing in python components")
    assert pipeline is not None
    # get default experiment
    experiment = utils.get_experiment_by_name()
    assert experiment is not None
    # start pipeline
    run = utils.start_pipeline(pipeline=pipeline, experiment=experiment, params={})
    assert run is not None
    # wait for completion
    status, error = utils.wait_pipeline_completion(run_id=run, wait=10)
    assert status.lower() == "succeeded"
    assert error == ""
