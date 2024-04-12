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

from configmaps import ConfigmapsManager
from kfp_support.api_server_client.params import ConfigMapVolume
from kfp_support.workflow_support.utils import RayRemoteJobs


def test_ray_remote_jobs():
    """
    Test the full cycle of job submission
    :return:
    """
    # This shows how to create volumes dictionary
    volumes = [
        ConfigMapVolume(
            name="code-sample",
            mount_path="/home/ray/samples",
            source="ray-job-code-sample",
            items={"sample_code.py": "sample_code.py"},
        )
    ]
    dct_volumes = {"volumes": [v.to_dict() for v in volumes]}

    head_node = {
        "cpu": 2,
        "memory": 4,
        "image": "rayproject/ray:2.9.0-py310",
        # Ray start params, just to show
        "ray_start_params": {"metrics-export-port": "8080", "num-cpus": "0", "dashboard-host": "0.0.0.0"},
    } | dct_volumes

    worker_node = {
        "cpu": 2,
        "memory": 4,
        "image": "rayproject/ray:2.9.0-py310",
        "replicas": 1,
        "min_replicas": 1,
        "max_replicas": 1,
    } | dct_volumes

    # Create configmap for testing
    cm_manager = ConfigmapsManager()
    cm_manager.delete_code_map()
    cm_manager.create_code_map()

    # create cluster
    remote_jobs = RayRemoteJobs(server_url="http://localhost:8080/ray")
    status, error = remote_jobs.create_ray_cluster(
        name="job-test", namespace="default", head_node=head_node, worker_nodes=[worker_node]
    )
    print(f"Created cluster - status: {status}, error: {error}")
    assert status == 200
    assert error is None
    # submitting ray job
    runtime_env = """
        pip:
          - requests==2.26.0
          - pendulum==2.1.2
        env_vars:
          counter_name: test_counter    
        """
    status, error, submission = remote_jobs.submit_job(
        name="job-test",
        namespace="default",
        request={},
        runtime_env=runtime_env,
        executor="/home/ray/samples/sample_code.py",
    )
    print(f"submit job - status: {status}, error: {error}, submission id {submission}")
    assert status == 200
    assert error is None
    # print execution log
    remote_jobs.follow_execution(name="job-test", namespace="default", submission_id=submission, print_timeout=20)
    # cleanup
    status, error = remote_jobs.delete_ray_cluster(name="job-test", namespace="default")
    print(f"Deleted cluster - status: {status}, error: {error}")
    assert status == 200
    assert error is None
