# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from argparse import Namespace
from typing import Any

from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import TransformStatistics
from data_processing.runtime.pure_python import (
    DefaultPythonTransformRuntime,
    PythonTransformRuntimeConfiguration,
    PythonTransformLauncher
)
from doc_id_transform_base import (
    IDGenerator,
    DocIDTransformBase,
    DocIDTransformConfigurationBase,
    start_id_key,
    id_generator_key
)


class DocIDTransform(DocIDTransformBase):
    """
    Implements schema modification of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        super().__init__(config)
        self.id_generator = config.get(id_generator_key, IDGenerator(config.get(start_id_key, 1)))

    def _get_starting_id(self, n_rows: int) -> int:
        """
        Get starting ID
        :param n_rows - number of rows in the table
        :return: starting id for the table
        """
        return self.id_generator.get_ids(n_rows=n_rows)


class DocIDTransformConfiguration(DocIDTransformConfigurationBase):

    def __init__(self):
        super().__init__(transform_class=DocIDTransform)

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        if args.runtime_num_processors > 0:
            self.logger.info(
                f"doc_id does not support multiprocessing. Runtime_num_processors should be 0, "
                f"current {args.runtime_num_processors}"
            )
            return False
        return super().apply_input_params(args=args)


class DocIDRuntime(DefaultPythonTransformRuntime):
    """
    Exact dedup runtime support
    """

    def __init__(self, params: dict[str, Any]):
        super().__init__(params=params)
        self.id_generator = None

    def get_transform_config(
            self, data_access_factory: DataAccessFactoryBase, statistics: TransformStatistics, files: list[str]
    ) -> dict[str, Any]:
        """
        Get the dictionary of configuration that will be provided to the transform's initializer.
        This is the opportunity for this runtime to create a new set of configuration based on the
        config/params provided to this instance's initializer.  This may include the addition
        of new configuration data such as ray shared memory, new actors, etc., that might be needed and
        expected by the transform in its initializer and/or transform() methods.
        :param data_access_factory - data access factory class being used by the RayOrchestrator.
        :param statistics - reference to statistics actor
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        self.id_generator = IDGenerator(self.params.get(start_id_key, 1))
        return self.params | {id_generator_key: self.id_generator}

    def compute_execution_stats(self, stats: TransformStatistics) -> None:
        """
        Update/augment the given statistics object with runtime-specific additions/modifications.
        :param stats: output of statistics as aggregated across all calls to all transforms.
        :return: job execution statistics.  These are generally reported as metadata by the Ray Orchestrator.
        """
        # compute and add additional statistics
        stats.add_stats({"final id": self.id_generator.get_current()})


class DocIDPythonTransformRuntimeConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=DocIDTransformConfiguration(),
            runtime_class=DocIDRuntime,
        )


if __name__ == "__main__":
    launcher = PythonTransformLauncher(DocIDPythonTransformRuntimeConfiguration())
    launcher.launch()
