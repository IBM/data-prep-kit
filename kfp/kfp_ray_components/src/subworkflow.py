import sys

from data_processing.utils import ParamsUtils
from runtime_utils import KFPUtils
from workflow_support.pipeline_utils import PipelinesUtils


def invoke_sub_workflow(
    name: str,  # workflow name
    prefix: str,  # workflow arguments prefix
    params: str = "",  # super workflow parameters containing parameters
    host: str = "http://ml-pipeline:8888",  # Host name
    np: int = 100,  # Number of pipelines to get
    exp_name: str = "Default",  # Experiment name
    tmout: int = -1,  # Time (mins) to wait for execution completion -1 - wait forever
    wait: int = 1,  # Frequency (min) to check execution status
    s3_input_prefix: str = "",
    output_folder: str = "",
) -> str:
    """
    Invoke kfp workflow run
    :param name: workflow name
    :param prefix: workflow arguments prefix
    :param params: super workflow parameters containing parameters
    :param host: kfp server name
    :param np: Number of pipelines to get
    :param exp_name: Experiment name
    :param tmout: Time (mins) to wait for execution completion -1 - wait forever
    :param wait: Frequency (min) to check execution status
    :param s3_input_prefix: input folder prefix
    :param output_folder: output folder path
    :return: Returns the output folder path
    """
    import time
    from pathlib import Path
    from typing import Any

    def _get_workflow_params(prefix: str, params: str) -> dict:
        """
        Construct a dictionary containing the workflow parameters.
        The workflow parameters are composed based on the following, arranged by their order of precedence:
        1.  Any parameter from the workflow `overriding_params`.
        2.  Parameters that are specific to the workflow (start with workflow arguments prefix).
        3.  Super pipeline parameters that start with "p2_pipeline_" prefix,
            which are common to all worklows.
        :param prefix: workflow parameters prefix
        :param params: super workflow parameters
        :return: workflow parameters as a dictionary
        """
        common_params_prefix = "p2_pipeline_"
        workflow_prms = {}
        params = params.replace('}"', "}")
        params = params.replace('"{', "{")
        dic = KFPUtils.load_from_json(params)
        for key in dic:
            # insert params with "p2_pipeline_" prefix
            if key.startswith(common_params_prefix):
                arg = key[len(common_params_prefix) :]
                if dic[key] != "":
                    workflow_prms[arg] = dic[key]
            if key.startswith(prefix):
                # insert workflow specific params
                arg = key[len(prefix) :]
                if dic[key] != "":
                    workflow_prms[arg] = dic[key]

        _add_overriding_params(dic, workflow_prms, prefix)
        return workflow_prms

    def _add_overriding_params(all_prms: dict, workflow_prms: dict, prefix: str) -> None:
        """
        Add workflow overriding params to workflow params, overriding values of existing params.
        :param all_prms: all super workflow parameters
        :param workflow_prms: The constructed workflow parameters
        :param prefix: workflow parameters prefix
        """
        workflow_overriding_params = all_prms.get(prefix + "overriding_params", {})
        for key, val in workflow_overriding_params.items():
            if isinstance(val, dict) and key in workflow_prms:
                # merge the dictionary values
                for k, v in val.items():
                    workflow_prms[key][k] = v
            else:
                workflow_prms[key] = val

    def _remove_unused_params(d: dict[str, Any]) -> None:
        d.pop("input_parent_path", None)
        d.pop("output_parent_path", None)
        d.pop("parent_path_suffix", None)
        d.pop("skip", None)
        d.pop("name", None)
        d.pop("overriding_params", None)
        return

    def _skip_task(prms) -> bool:
        # if there is a skip parameter and it is True then skip the task
        if "skip" in prms:
            if prms.get("skip", "False").lower() in ("true", "yes"):
                return True
        return False

    print(f"input path = {s3_input_prefix}")
    print(f"Invoking sub workflow {name}, host {host}")
    start = time.time()

    # Get params
    if params == "":
        # No parameters - use defaults
        prm = None
    else:
        prm = _get_workflow_params(prefix, params)
    data_s3_config: dict[str, str] = None
    if s3_input_prefix != "":
        if s3_input_prefix[-1] == "/":
            s3_input_prefix = s3_input_prefix[:-1]
        input_folder = s3_input_prefix + "/" + prm.get("parent_path_suffix", "")

    output_parent_path = prm.get("output_parent_path", "")

    # The output path includes all the tasks upstream.
    if s3_input_prefix.startswith(output_parent_path):
        output_folder_prefix = s3_input_prefix + "_" + prm.get("name", "")
    else:
        output_folder_prefix = output_parent_path + "_" + prm.get("name", "")
    output_folder = output_folder_prefix + "/" + prm.get("parent_path_suffix", "")

    input_folder = input_folder.replace('"', "'")
    output_folder = output_folder.replace('"', "'")
    data_s3_config = {"input_folder": input_folder, "output_folder": output_folder}
    prm["data_s3_config"] = data_s3_config
    # Check if to skip preprocessing
    if _skip_task(prm):
        print("skipped preprocess step")
        output_folder = s3_input_prefix
        Path(args.output_folder).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_folder).write_text(output_folder)
        return output_folder

    _remove_unused_params(prm)

    for key, value in prm.items():
        if isinstance(value, dict):
            prm[key] = ParamsUtils.convert_to_ast(value)

    print(f"start pipeline {name} with parameters {prm}")

    utils = PipelinesUtils(host="http://ml-pipeline:8888")
    # Get experiment

    # get experiment
    experiment = utils.get_experiment_by_name(exp_name)
    if experiment is None:
        print(f"Failed to get an experiment")
        sys.exit(1)
    print("Got experiment")

    # Get pipeline
    p = utils.get_pipeline_by_name(name=name, np=np)
    if p is None:
        print(f"Failed to get pipeline {name} with the amount of gets {np}")
        sys.exit(1)
    print(f"Got pipeline {p.name}")

    # Start execution
    run_id = utils.start_pipeline(p, experiment, prm)
    if run_id is None:
        print(f"Failed to start sub workflow {name}")
        sys.exit(1)
    print(f"Started pipeline execution. Run id {run_id}")

    # Wait for completion
    status, error = utils.wait_pipeline_completion(run_id=run_id, timeout=tmout, wait=wait)
    if status.lower() not in ["succeeded", "completed"]:
        # Execution failed
        print(f"Sub workflow {name} execution failed with error {error} and status {status}")
        sys.exit(1)

    print(f"Sub workflow {name} execution completed successfully in {round((time.time() - start) / 60.,3)} min")
    print(f"output path = {output_folder_prefix}")
    Path(args.output_folder).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_folder).write_text(output_folder_prefix)
    return output_folder_prefix


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start sub workflow")
    parser.add_argument("--name", type=str)
    parser.add_argument("--prefix", type=str)
    parser.add_argument("--params", type=str, default=None)
    parser.add_argument("--host", type=str, default="http://ml-pipeline:8888")
    parser.add_argument("--number_pipelines", type=int, default=100)
    parser.add_argument("--experiment", type=str, default="Default")
    parser.add_argument("--execution_timeout", type=int, default=-1)
    parser.add_argument("--time_interval", type=int, default=1)
    parser.add_argument("-if", "--input_folder", default="", type=str)
    parser.add_argument("-of", "--output_folder", default="", type=str)

    args = parser.parse_args()

    invoke_sub_workflow(
        name=args.name,
        prefix=args.prefix,
        params=args.params,
        host=args.host,
        np=args.number_pipelines,
        exp_name=args.experiment,
        tmout=args.execution_timeout,
        wait=args.time_interval,
        s3_input_prefix=args.input_folder,
        output_folder=args.output_folder,
    )
