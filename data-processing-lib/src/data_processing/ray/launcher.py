import time

import ray
from cli_arg_provider import *
from data_access_factory import DataAccessFactory
from data_transformer import AbstractDataTransformRuntimeFactory
from transformer_orchestrator import transform_orchestrator


class TransformerLauncher:
    """
    Driver class starting Filter execution
    """

    def __init__(self, name: str, transformer_factory: AbstractDataTransformRuntimeFactory):
        """
        Creates driver
        :param name: name of the application
        :param transformer_factory: transformer runtime factory
        """
        self.name = name
        self.transformer_factory = transformer_factory
        self.data_access_factory = DataAccessFactory()
        self.ray_orchestrator = RayOrchestratorConfiguration(name=name)

    def __get_parameters(self) -> bool:
        """
        This method creates arg parser, fill it with the parameters
        and does parameters validation
        :return: True id validation passe or False, if not
        """
        parser = argparse.ArgumentParser(description=f"Driver for {self.name} processing")
        parser.add_argument(
            "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="running local flag"
        )
        # add additional arguments
        self.transformer_factory.define_input_params(parser=parser)
        self.data_access_factory.define_input_params(parser=parser)
        self.ray_orchestrator.define_input_params(parser=parser)
        args = parser.parse_args()
        self.run_locally = args.run_locally
        return (
            self.transformer_factory.validate_input_params(args=args)
            and self.data_access_factory.validate_input_params(args=args)
            and self.ray_orchestrator.validate_input_params(args=args)
        )

    def submit_for_execution(self) -> int:
        """
        Submit for Ray execution
        :return:
        """
        res = 1
        start = time.time()
        try:
            if self.run_locally:
                # Will create a local Ray cluster
                print("running locally creating Ray cluster")
                ray.init()
            else:
                # connect to the existing cluster
                print("Connecting to the existing Ray cluster")
                ray.init(f"ray://localhost:10001", ignore_reinit_error=True)
            res = ray.get(
                transform_orchestrator.remote(
                    preprocessing_params=self.ray_orchestrator,
                    data_access_factory=self.data_access_factory,
                    transformer_runtime_factory=self.transformer_factory,
                )
            )
        finally:
            print(f"Completed execution in {(time.time() - start)/60.} min, execution result {res}")
            ray.shutdown()
            return res

    def execute(self) -> int:
        """
        Execute method orchestrates driver invocation
        :return:
        """
        if self.__get_parameters():
            return self.submit_for_execution()
        return 1
