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
import json
import os
import sys

from runtime_utils import KFPUtils, RayRemoteJobs


def start_ray_cluster(
    name: str,  # name of Ray cluster
    ray_head_options: str,  # ray head configuration
    ray_worker_options: str,  # ray worker configuration
    server_url: str,  # url of api server
    additional_params: str,  # additional parameters for
) -> None:
    """
    Create Ray cluster
    :param name: cluster name
    :param ray_head_options: head node options
    :param ray_worker_options: worker node option (here we assume only 1 worker group)
    :param server_url: API server URL
    :param additional_params:  additional parameters
    :return: None
    """
    dict_params = KFPUtils.load_from_json(additional_params.replace("'", '"'))
    # get current namespace
    ns = KFPUtils.get_namespace()
    if ns == "":
        print(f"Failed to get namespace")
        sys.exit(1)
    # Convert input
    head_options = KFPUtils.load_from_json(ray_head_options.replace("'", '"'))
    worker_node = KFPUtils.load_from_json(ray_worker_options.replace("'", '"'))
    head_node = head_options | {
        "ray_start_params": {"metrics-export-port": "8080", "num-cpus": "0", "dashboard-host": "0.0.0.0"}
    }
    tolerations = os.getenv("KFP_TOLERATIONS", "")
    if tolerations != "":
        print(f"Adding tolerations {tolerations} for ray pods")
        tolerations = json.loads(tolerations)
        if "tolerations" in head_node:
            print("Warning: head_node tolerations already defined, will overwrite it")
        if "tolerations" in worker_node:
            print("Warning: worker_node tolerations already defined, will overwrite it")
        head_node["tolerations"] = tolerations
        worker_node["tolerations"] = tolerations
    # create cluster
    remote_jobs = RayRemoteJobs(
        server_url=server_url,
        http_retries=dict_params.get("http_retries", 5),
        wait_interval=dict_params.get("wait_interval", 2),
    )
    status, error = remote_jobs.create_ray_cluster(
        name=name,
        namespace=ns,
        head_node=head_node,
        worker_nodes=[worker_node],
        wait_cluster_ready=dict_params.get("wait_cluster_ready_tmout", -1),
    )

    if status != 200:
        print(f"Failed to created cluster - status: {status}, error: {error}")
        exit(1)

    print(f"Created cluster successfully")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start Ray cluster operation")
    parser.add_argument("-rn", "--ray_name", type=str, default="")
    parser.add_argument("-id", "--run_id", type=str, default="")
    parser.add_argument("-ho", "--ray_head_options", default="{}", type=str)
    parser.add_argument("-wo", "--ray_worker_options", default="{}", type=str)
    parser.add_argument("-su", "--server_url", default="", type=str)
    parser.add_argument("-ap", "--additional_params", default="{}", type=str)

    args = parser.parse_args()

    cluster_name = KFPUtils.runtime_name(
        ray_name=args.ray_name,
        run_id=args.run_id,
    )

    start_ray_cluster(
        name=cluster_name,
        ray_head_options=args.ray_head_options,
        ray_worker_options=args.ray_worker_options,
        server_url=args.server_url,
        additional_params=args.additional_params,
    )
