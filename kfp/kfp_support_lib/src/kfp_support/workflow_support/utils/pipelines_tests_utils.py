import os

from data_processing.utils import get_logger, str2bool

from . import PipelinesUtils


logger = get_logger(__name__)


def run_test(pipeline_package_path: str, endpoint: str = "http://localhost:8080/kfp", overwrite: bool = True):
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
    pipeline = utils.upload_pipeline(
        pipeline_package_path=pipeline_package_path,
        pipeline_name=pipeline_name,
        overwrite=overwrite,
    )
    if pipeline is None:
        return None
    experiment = utils.get_experiment_by_name()
    run_id = utils.start_pipeline(pipeline, experiment, params=[])
    status, error = utils.wait_pipeline_completion(run_id=run_id, timeout=tmout, wait=wait)
    if status.lower() not in ["succeeded", "completed"]:
        # Execution failed
        logger.warning(f"Pipeline {pipeline_name} failed with error {error} and status {status}")
        return None
    logger.info(f"Pipeline {pipeline_name} successfully completed")
    return pipeline_name


def run_sanity_test(
    pipeline_package_path: str = "", endpoint: str = "http://localhost:8080/kfp", overwrite: bool = True
):
    """
    Run sanity test: automatic upload and run a pipeline.

    :param pipeline_package_path: Local path to the pipeline package.
    :param endpoint: endpoint to kfp service
    """
    pipeline_run = run_test(pipeline_package_path, endpoint=endpoint, overwrite=overwrite)
    if pipeline_run is not None:
        logger.info(f"{pipeline_run} run successfully launched")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run sanity test")
    parser.add_argument("-c", "--command", type=str, choices=["upload", "sanity-test"])
    parser.add_argument("-e", "--endpoint", type=str, default="http://localhost:8080/kfp")
    parser.add_argument("-p", "--pipeline_package_path", type=str, default="")
    parser.add_argument("-o", "--overwrite", type=str, default="True")

    args = parser.parse_args()
    match args.command:
        case "upload":
            file_name = os.path.basename(args.pipeline_package_path)
            pipeline_name = os.path.splitext(file_name)[0]
            utils = PipelinesUtils(host=args.endpoint)
            utils.upload_pipeline(
                pipeline_package_path=args.pipeline_package_path,
                pipeline_name=pipeline_name,
                overwrite=str2bool(args.overwrite),
            )
        case "sanity-test":
            run_sanity_test(
                endpoint=args.endpoint,
                pipeline_package_path=args.pipeline_package_path,
                overwrite=str2bool(args.overwrite),
            )
        case _:
            logger.warning("Unsupported command")
            exit(1)
