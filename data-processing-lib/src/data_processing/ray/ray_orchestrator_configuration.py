from fm_data_processing.cli import *


class RayOrchestratorConfiguration(CLIArgumentProvider):
    """
    A class specifying and validating Ray orchestrator configuration
    """

    def __init__(self, name: str):
        """
        Initialization
        """
        self.worker_options = {}
        self.n_workers = 1
        self.creation_delay = 0
        self.pipeline_id = ""
        self.job_details = {}
        self.code_location = {}
        self.name = name

    def define_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        This method adds transformer specific parameter to parser
        :param parser: parser
        :return:
        """
        parser.add_argument("--num_workers", type=int, default=1, help="number of workers")
        """ 
        ASR defining worker resource requirements and can contain the following keys:
            num_cpus - required number of cpus
            num_gpus - required number of gpus
            resources - required list of custom resources, 
                for example: resources:{"special_hardware": 1, "custom_label": 1}
            The complete list can be found at 
            https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options
            and contains
            accelerator_type, memory, name, num_cpus, num_gpus, object_store_memory, placement_group, 
            placement_group_bundle_index, placement_group_capture_child_tasks, resources, runtime_env, 
            scheduling_strategy, _metadata, concurrency_groups, lifetime, max_concurrency, max_restarts, 
            max_task_retries, max_pending_calls, namespace, get_if_exists
        """
        parser.add_argument(
            "--worker_options",
            type=ast.literal_eval,
            default="{'num_cpus': 0.8}",
            help="ast string of options for worker execution",
        )
        parser.add_argument("--pipeline_id", type=str, default="pipeline_id", help="pipeline id")
        parser.add_argument("--job_id", type=str, default="job_id", help="job id")
        parser.add_argument("--creation_delay", type=int, default=0, help="delay between actor' creation")
        """ 
        AST defining code location should contain the following keys:
            github - github location
            commit_hash - commit hash
            path - path within github
        """
        parser.add_argument(
            "--code_location",
            type=ast.literal_eval,
            default="{'github': 'github', 'commit_hash': '12345', 'path': 'path'}",
            help="ast string containing code location",
        )

    def validate_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate transformer specific parameters
        :param args: user defined arguments
        :return: True, if validate pass or False otherwise
        """
        # store parameters locally
        self.worker_options = args.worker_options
        self.n_workers = args.num_workers
        self.creation_delay = args.creation_delay
        self.pipeline_id = args.pipeline_id
        self.job_details = {
            "job category": "preprocessing",
            "job name": self.name,
            "job type": "ray",
            "job id": args.job_id,
        }
        self.code_location = (args.code_location,)

        # print them
        print(f"worker options {self.worker_options}")
        print(f"pipeline id {self.pipeline_id}; number workers {self.n_workers}")
        print(f"job details {self.job_details}")
        print(f"code location {self.code_location}")
        print(f"actor creation delay {self.creation_delay}")
        return True
