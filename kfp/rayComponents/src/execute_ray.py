# execute Ray jobs
import json
import sys
from typing import Any

def dict_to_exec_str(executor: str, d: dict[str, Any]) -> str:
    res = f"python {executor} "
    for key, value in d.items():
        if isinstance(value, str):
            res += f'--{key}="{value}" '
        else:
            res += f"--{key}={value} "
    return res

# Load a string that represents a json to python dictionary
def load_from_json(js: str) -> dict[str, Any]:
    try:
        return json.loads(js)
    except Exception as e:
        print(f"Failed to load parameters {js} with error {e}")
        sys.exit(1)

def execute_ray_jobs(
    name: str,  # ray dashboard uri
    additional_params: str,
    exec_params: str,  # task parameters
    server_url: str,
):
    import sys

    from kfp_support.workflow_support.utils import RayRemoteJobs, KFPUtils

    # get current namespace
    ns = KFPUtils.get_namespace()
    if ns == "":
        print(f"Failed to get namespace")
        sys.exit(1)
    remote_jobs = RayRemoteJobs(server_url=server_url)

    # Get parameters necessary for submitting
    additional_params_dict = load_from_json(additional_params)
    exec_params = load_from_json(exec_params)
    print(f"exec params is {exec_params}")

    access_key, secret_key, cos_url = KFPUtils.credentials()
    exec_params["s3_cred"] = "{'access_key': '" + access_key + "', 'secret_key': '" + secret_key + "', 'cos_url': '" + cos_url + "'}"

    print(f"\nexec params is {exec_params}")
    print(f"\nrequest {dict_to_exec_str(additional_params_dict.get('script_name', ''), exec_params)}")

    # KFPUtils.clean_standard_prefixes(exec_params)

    # execute cluster
    status, error, submission = remote_jobs.submit_job(name=name, namespace=ns, request=exec_params,
                                                       executor=additional_params_dict.get("script_name", ""))
    print(f"submit job - status: {status}, error: {error}, submission id {submission}")

    # print execution log
    remote_jobs.follow_execution(name=name, namespace=ns, submission_id=submission, print_timeout=20)

if __name__ == "__main__":
    import argparse

    from kfp_support.workflow_support.utils import KFPUtils

    parser = argparse.ArgumentParser(description="Execute Ray job operation")
    parser.add_argument("-rn", "--ray_name", type=str, default="")
    parser.add_argument("-id", "--run_id", type=str, default="")
    parser.add_argument("-ap", "--additional_params", type=str, default="{}")
    parser.add_argument("-n", "--notifier_str", type=str, default="")
    # The component converts the dictionary to json string
    parser.add_argument("-ep", "--exec_params", type=str, default="{}")
    parser.add_argument("-su", "--server_url", default="", type=str)

    args = parser.parse_args()

    cluster_name = KFPUtils.runtime_name(
        ray_name=args.ray_name,
        run_id=args.run_id,
    )

    execute_ray_jobs(
        name=cluster_name,
        additional_params=args.additional_params,
        exec_params=args.exec_params,
        server_url=args.server_url,
    )
