import argparse
import time

import ray
from data_processing.data_access import DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    TransformOrchestratorConfiguration,
    orchestrate,
)
from data_processing.utils import str2bool


class TransformLauncher:
    """
    Driver class starting Filter execution
    """

    def __init__(
        self,
        name: str,
        transform_runtime_config: DefaultTableTransformConfiguration,
        data_access_factory: DataAccessFactory = DataAccessFactory(),
    ):
        """
        Creates driver
        :param name: name of the application
        :param transform_runtime_config: transform runtime factory
        :param data_access_factory: the factory to create DataAccess instances.
        """
        self.name = name
        self.transform_runtime_config = transform_runtime_config
        self.data_access_factory = data_access_factory
        self.ray_orchestrator = TransformOrchestratorConfiguration(name=name)

    def __get_parameters(self) -> bool:
        """
        This method creates arg parser, fill it with the parameters
        and does parameters validation
        :return: True id validation passe or False, if not
        """
        parser = argparse.ArgumentParser(description=f"Driver for {self.name} processing")
        parser.add_argument(
            "--run_locally", type=lambda x: bool(str2bool(x)), default=False, help="running ray local flag"
        )
        # add additional arguments
        self.transform_runtime_config.add_input_params(parser=parser)
        self.data_access_factory.add_input_params(parser=parser)
        self.ray_orchestrator.add_input_params(parser=parser)
        args = parser.parse_args()
        self.run_locally = args.run_locally
        if self.run_locally:
            print("running locally")
        else:
            print("connecting to existing cluster")
        return (
            self.transform_runtime_config.apply_input_params(args=args)
            and self.data_access_factory.apply_input_params(args=args)
            and self.ray_orchestrator.apply_input_params(args=args)
        )

    def _submit_for_execution(self) -> int:
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
                orchestrate.remote(
                    preprocessing_params=self.ray_orchestrator,
                    data_access_factory=self.data_access_factory,
                    transform_runtime_config=self.transform_runtime_config,
                )
            )
            time.sleep(10)
        except Exception as e:
            print(f"Exception running ray remote orchestration\n{e}")
        finally:
            print(f"Completed execution in {(time.time() - start)/60.} min, execution result {res}")
            ray.shutdown()
            return res

    def launch(self) -> int:
        """
        Execute method orchestrates driver invocation
        :return:
        """
        if self.__get_parameters():
            return self._submit_for_execution()
        return 1
