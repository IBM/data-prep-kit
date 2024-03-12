# Cleans and shutdowns the Ray cluster
def cleanup_ray_cluster(
    name: str,  # name of Ray cluster
    server_url: str, # url of api server
):
    import sys

    from kfp_support.workflow_support.utils import RayRemoteJobs, KFPUtils


    # get current namespace
    ns = KFPUtils.get_namespace()
    if ns == "":
        print(f"Failed to get namespace")
        sys.exit(1)
    
    # create cluster
    remote_jobs = RayRemoteJobs(server_url=server_url)

    # cleanup
    status, error = remote_jobs.delete_ray_cluster(name=name, namespace=ns)
    print(f"Deleted cluster - status: {status}, error: {error}")


if __name__ == "__main__":
    import argparse

    from kfp_support.workflow_support.utils import KFPUtils

    parser = argparse.ArgumentParser(description="Stop Ray cluster operation")
    parser.add_argument("-rn", "--ray_name", type=str, default="")
    parser.add_argument("-id", "--run_id", type=str, default="")
    parser.add_argument("-su", "--server_url", default="", type=str)
    args = parser.parse_args()

    cluster_name = KFPUtils.runtime_name(
        ray_name=args.ray_name,
        run_id=args.run_id,
    )

    cleanup_ray_cluster(
        name=cluster_name,
        server_url=args.server_url,
    )
