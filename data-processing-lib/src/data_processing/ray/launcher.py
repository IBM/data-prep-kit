import argparse
import time

import ray

import data_processing
from data_processing.data_access.data_access_factory import DataAccessFactory
from data_processing.ray.ray_orchestrator_configuration import RayOrchestratorConfiguration
from data_processing.ray.transform_runtime import AbstractTableTransformRuntimeFactory
from data_processing.utils.cli import str2bool



class TransformLauncher:
    """
    Driver class starting Filter execution
    """

    def __init__(self, name: str, transform_factory: AbstractTableTransformRuntimeFactory,
                 data_access_factory_class:type[DataAccessFactory]= None):
        """
        Creates driver
        :param name: name of the application
        :param transform_factory: transform runtime factory
        :param data_access_factory_class: the factory to create DataAccess instances.
        """
        self.name = name
        self.transform_factory = transform_factory
        if data_access_factory_class is None:
            self.data_access_factory = DataAccessFactory()
        else:
            self.data_access_factory = data_access_factory_class()
        self.ray_orchestrator = RayOrchestratorConfiguration(name=name)
        self.transform_runtime = self.transform_factory.create_transform_runtime()
        self.parsed_args = {}

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
        self.transform_factory.add_input_params(parser=parser)
        self.transform_runtime.add_input_params(parser=parser)
        self.data_access_factory.add_input_params(parser=parser)
        self.ray_orchestrator.add_input_params(parser=parser)
        args = parser.parse_args()
        self.parsed_args = args
        self.run_locally = args.run_locally
        return (
            self.transform_factory.apply_input_params(args=args)
            and self.transform_runtime.apply_input_params(args=args)
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
                data_processing.ray.transform_orchestrator.transform_orchestrator.remote(
                    vars(self.parsed_args),
                    preprocessing_params=self.ray_orchestrator,
                    data_access_factory=self.data_access_factory,
                    transform_runtime_factory=self.transform_factory,
                )
            )
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
