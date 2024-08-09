from multiprocessing import Pool
from typing import Any
import time

from data_processing.data_access import DataAccessFactoryBase
from data_processing.runtime import AbstractTransformFileProcessor
from data_processing.runtime.pure_python import PythonTransformRuntimeConfiguration
from data_processing.transform import TransformStatistics
from data_processing.utils import get_logger


logger = get_logger(__name__)


class PythonPoolTransformFileProcessor(AbstractTransformFileProcessor):
    """
    This is the class implementing the worker class processing of a single file
    """

    def __init__(
            self,
            data_access_factory: DataAccessFactoryBase,
            runtime_configuration: PythonTransformRuntimeConfiguration,
    ):
        """
        Init method
        :param data_access_factory - data access factory
        :param runtime_configuration: transform configuration class
        """
        super().__init__(
            data_access_factory=data_access_factory,
            transform_parameters=dict(runtime_configuration.get_transform_params()),
        )
        # Add data access and statistics to the processor parameters
        self.transform_params["data_access"] = self.data_access
        self.transform_class = runtime_configuration.get_transform_class()
        self.transform = None

    def process_file(self, f_name: str) -> dict[str, Any]:
        # re initialize statistics
        self.stats = {}
        if self.transform is None:
            # create transform. Make sure to do this locally
            self.transform = self.transform_class(self.transform_params)
        # Invoke superclass method
        super().process_file(f_name=f_name)
        # return collected statistics
        return self.stats

    def flush(self) -> dict[str, Any]:
        # re initialize statistics
        self.stats = {}
        # Invoke superclass method
        super().flush()
        # return collected statistics
        return self.stats

    def _publish_stats(self, stats: dict[str, Any]) -> None:
        """
        Publish statistics (to the local dictionary)
        :param stats: statistics dictionary
        :return: None
        """
        for key, val in stats.items():
            # for all key/values
            if val > 0:
                # for values greater then 0
                self.stats[key] = self.stats.get(key, 0) + val


def process_transforms(files: list[str], size: int, print_interval: int, data_access_factory: DataAccessFactoryBase,
                       runtime_configuration: PythonTransformRuntimeConfiguration) -> TransformStatistics:
    """
    Process transforms using multiprocessing pool
    :param files: list of files to process
    :param size: pool size
    :param print_interval: print interval
    :param data_access_factory: data access factory
    :param runtime_configuration: transform configuration
    :return: metadata for the execution
    """
    # result statistics
    statistics = TransformStatistics()
    # create processor
    processor = PythonPoolTransformFileProcessor(data_access_factory=data_access_factory,
                                                 runtime_configuration=runtime_configuration)
    completed = 0
    t_start = time.time()
    # create multiprocessing pool
    with Pool(processes=size) as pool:
        # execute for every input file
        for result in pool.imap_unordered(processor.process_file, files):
            completed += 1
            # accumulate statistics
            statistics.add_stats(result)
            if completed % print_interval == 0:
                # print intermediate statistics
                logger.info(
                    f"Completed {completed} files ({100 * completed / len(files)}%) "
                    f"in {(time.time() - t_start)/60} min"
                )
        logger.info(f"Done processing {completed} files, waiting for flush() completion.")
        results = [{}] * size
        # flush
        for i in range(size):
            results[i] = pool.apply_async(processor.flush)
        for s in results:
            statistics.add_stats(s.get())
    return statistics
