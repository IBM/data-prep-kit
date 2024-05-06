import os

from . import PipelinesUtils


def upload_pipeline(
    pipeline_name: str,
    pipeline_package_path: str,
    utils: PipelinesUtils = None,
    endpoint: str = "http://localhost:8080/kfp",
):
    """
    Upload a pipeline. If the pipeline exists delete it and redeploy.

    :param utils: object of type PipelinesUtils
    :param pipeline_name: Name of the pipeline to be shown in the UI.
    :param pipeline_package_path: Name of the pipeline to be shown in the UI.
    :param utils: Optional. Object of type PipelinesUtils.
    :param endpoint: Optional. endpoint to kfp service.
    return: Server response object containing pipleine id and other information.
    """
    if utils is None:
        utils = PipelinesUtils(host=endpoint)
    pipeline = utils.get_pipeline_by_name(name=pipeline_name)
    if pipeline is not None:
        try:
            print(f"pipeline {pipeline_name} already exists. Trying to delete it.")
            utils.delete_pipeline(pipeline_id=pipeline.id)
        except Exception as e:
            print(f"Exception deleting pipeline {e}")
            return None
    pipeline = utils.upload_pipeline(pipeline_package_path=pipeline_package_path, pipeline_name=pipeline_name)
    if pipeline is None:
        print(f"Failed to upload pipeline {pipeline_name}.")
        return None

    print(f"Pipeline {pipeline.id} uploaded successfully")
    return pipeline


def run_test(pipeline_package_path: str, endpoint: str = "http://localhost:8080/kfp"):
    """
    Upload and run a single pipeline

    :param pipeline_package_path: Local path to the pipeline package.
    :param endpoint: endpoint to kfp service.
    :return the pipeline name as it appears in the kfp GUI.
    """
    tmout: int = 800
    wait: int = 60
    file_name = os.path.basename(pipeline_package_path)
    pipeline_name = os.path.splitext(file_name)[0]
    utils = PipelinesUtils(host=endpoint)
    pipeline = upload_pipeline(utils=utils, pipeline_package_path=pipeline_package_path, pipeline_name=pipeline_name)
    if pipeline is None:
        return None
    experiment = utils.get_experiment_by_name()
    run_id = utils.start_pipeline(pipeline, experiment, params=[])
    status, error = utils.wait_pipeline_completion(run_id=run_id, timeout=tmout, wait=wait)
    if status.lower() not in ["succeeded", "completed"]:
        # Execution failed
        print(f"Pipeline {pipeline_name} failed with error {error} and status {status}")
        return None
    print(f"Pipeline {pipeline_name} successfully completed")
    return pipeline_name


def run_sanity_test(pipeline_package_path: str = "", endpoint: str = "http://localhost:8080/kfp"):
    """
    Run sanity test: automatic upload and run a pipeline.

    :param pipeline_package_path: Local path to the pipeline package.
    :param endpoint: endpoint to kfp service
    """
    pipeline_run = run_test(pipeline_package_path, endpoint=endpoint)
    if pipeline_run is not None:
        print(f"{pipeline_run} run successfully launched")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run sanity test")
    parser.add_argument("-c", "--command", type=str, choices=["upload", "sanity-test"])
    parser.add_argument("-e", "--endpoint", type=str, default="http://localhost:8080/kfp")
    parser.add_argument("-p", "--pipeline_package_path", type=str, default="")

    args = parser.parse_args()
    match args.command:
        case "upload":
            file_name = os.path.basename(args.pipeline_package_path)
            pipeline_name = os.path.splitext(file_name)[0]
            upload_pipeline(pipeline_package_path=args.pipeline_package_path, pipeline_name=pipeline_name)
        case "sanity-test":
            run_sanity_test(endpoint=args.endpoint, pipeline_package_path=args.pipeline_package_path)
        case _:
            print("Unsupported command")
            exit(1)
