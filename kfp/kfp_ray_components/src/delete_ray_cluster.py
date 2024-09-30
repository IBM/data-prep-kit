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
import time

from runtime_utils import KFPUtils, RayRemoteJobs


# Cleans and shutdowns the Ray cluster
def cleanup_ray_cluster(
    name: str,  # name of Ray cluster
    server_url: str,  # url of api server
    additional_params: str,  # additional parameters for
):
    # get current namespace
    ns = KFPUtils.get_namespace()
    if ns == "":
        print(f"Failed to get namespace")
        sys.exit(1)

    dict_params = KFPUtils.load_from_json(additional_params.replace("'", '"'))
    delete_cluster_delay_minutes = dict_params.get("delete_cluster_delay_minutes", 0)
    time.sleep(delete_cluster_delay_minutes * 60)
    # cleanup
    remote_jobs = RayRemoteJobs(server_url=server_url)
    status, error = remote_jobs.delete_ray_cluster(name=name, namespace=ns)
    print(f"Deleted cluster - status: {status}, error: {error}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stop Ray cluster operation")
    parser.add_argument("-rn", "--ray_name", type=str, default="")
    parser.add_argument("-id", "--run_id", type=str, default="")
    parser.add_argument("-su", "--server_url", default="", type=str)
    parser.add_argument("-ap", "--additional_params", default="{}", type=str)
    args = parser.parse_args()

    cluster_name = KFPUtils.runtime_name(
        ray_name=args.ray_name,
        run_id=args.run_id,
    )

    cleanup_ray_cluster(
        name=cluster_name,
        server_url=args.server_url,
        additional_params=args.additional_params,
    )
