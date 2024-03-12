def start_ray_cluster(
    name: str,  # name of Ray cluster
    num_workers: int,  # number of workers
    cpus: int,  # cpus per worker
    memory: int,  # memory per worker
    num_gpus: int,  # number of gpus per worker
    image: str,  # image for Ray cluster
    server_url: str, # url of api server
    additional_params: str, # additional parameters for 
):
    import json
    import sys

    from kfp_support.workflow_support.utils import RayRemoteJobs, KFPUtils

    try:
        dict_params = json.loads(additional_params)
    except Exception as e:
        print(f"Failed to load additional parameters {additional_params} with error {e}")
        sys.exit(1)

    # get current namespace
    ns = KFPUtils.get_namespace()
    if ns == "":
        print(f"Failed to get namespace")
        sys.exit(1)

    head_node = {
        "cpu": cpus,
        "memory": memory,
        "image": image,
        "image_pull_secret": dict_params.get("image_pull_secret", "prod-all-icr-io"),
        # Ray start params, just to show
        "ray_start_params": {
            "metrics-export-port": "8080",
            "num-cpus": "0",
            "dashboard-host": "0.0.0.0"
        },
    }

    worker_node = {
        "cpu": cpus,
        "memory": memory,
        "image": image,
        "replicas": num_workers,
        "min_replicas": num_workers,
        "max_replicas": num_workers,
        "image_pull_secret": dict_params.get("image_pull_secret", "prod-all-icr-io"),
    }

    # create cluster
    remote_jobs = RayRemoteJobs(server_url=server_url)
    status, error = remote_jobs.create_ray_cluster(name=name, namespace=ns, head_node=head_node,
                                                   worker_nodes=[worker_node])
    
    print(f"Created cluster - status: {status}, error: {error}")

if __name__ == "__main__":
    import argparse

    from kfp_support.workflow_support.utils import KFPUtils

    parser = argparse.ArgumentParser(description="Start Ray cluster operation")
    parser.add_argument("-rn", "--ray_name", type=str, default="")
    parser.add_argument("-id", "--run_id", type=str, default="")
    parser.add_argument("-w", "--num_workers", default=1, type=int)
    parser.add_argument("--cpus", default=2, type=int)
    parser.add_argument("-m", "--memory", default=16, type=int)
    parser.add_argument("-g", "--num_gpus", default=0, type=int)
    parser.add_argument("-img", "--image", default="", type=str)
    parser.add_argument("-su", "--server_url", default="", type=str)
    parser.add_argument("-ap", "--additional_params", default="{}", type=str)

    args = parser.parse_args()

    cluster_name = KFPUtils.runtime_name(
        ray_name=args.ray_name,
        run_id=args.run_id,
    )

    start_ray_cluster(
        name=cluster_name,
        num_workers=args.num_workers,
        cpus=args.cpus,
        memory=args.memory,
        num_gpus=args.num_gpus,
        image=args.image.strip(),
        server_url=args.server_url,
        additional_params=args.additional_params,
    )
