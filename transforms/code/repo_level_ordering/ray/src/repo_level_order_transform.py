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

import datetime
import os
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
import ray
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger
from data_processing_ray.runtime.ray import (
    DefaultRayTransformRuntime,
    RayTransformLauncher,
    RayUtils,
)
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from dpk_repo_level_order.internal.store.store_factory import (
    create_store,
    create_store_params,
    init_store_params,
    store_type_value_local,
    store_type_value_ray,
    store_type_value_s3,
)
from ray.actor import ActorHandle


short_name = "repo_lvl"
cli_prefix = f"{short_name}_"
pwd_key = "pwd"
pwd_cli_param = f"{cli_prefix}{pwd_key}"

grouping_column_key = "grouping_column"
store_params_key = "store_params"

store_type_key = "store_type"
store_dir_key = "store_backend_dir"
store_s3_secret_key = "other_s3_secret"
store_s3_keyid_key = "other_s3_keyid"
store_s3_url_key = "other_s3_url"
store_ray_cpus_key = "store_ray_cpus"
store_ray_nworkers_key = "store_ray_nworkers"

sorting_enable_key = "sorting_enabled"
sorting_algo_key = "sorting_algo"

output_by_langs_key = "output_by_langs"
output_superrows_key = "combine_rows"


@ray.remote
def add_file(store, group, file_name):
    store.put(group, file_name)


class RepoLevelOrderTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, RepoLevelOrderTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of RepoLevelOrderTransformConfiguration class
        super().__init__(config)

        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        self.config = config
        self.grouping_column = config.get(grouping_column_key)
        store_params = config.get(store_params_key)
        self.store = create_store(store_params)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        grouped_dataframe = table.to_pandas().groupby(self.grouping_column)

        results = []
        for group, _ in grouped_dataframe:
            # This supports only flat folder structure, so all
            # files should be in the same folder
            # since store uses filesystem as backend
            # can't store full path in store since, store is currently flat filesystem.
            file_name = os.path.basename(file_name)
            # self.store.put(group, file_name)
            results.append(add_file.remote(self.store, group, file_name))

        ray.get(results)
        stats = {"identified_groups": len(grouped_dataframe)}
        self.logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)} | stats
        return [], metadata


class RepoLevelOrderRuntime(DefaultRayTransformRuntime):
    """
    RepoLevelOrder runtime support
    """

    def __init__(self, params: dict[str, Any]):
        self.logger = get_logger(__name__)
        super().__init__(params)

        self.stage_one_only = self.params["stage_one_only"]
        self.sorting_enabled = self.params[sorting_enable_key]
        self.sorting_algo = self.params[sorting_algo_key]
        self.output_by_langs = self.params[output_by_langs_key]
        self.combine_rows = self.params[output_superrows_key]
        self.ray_workers = self.params["ray_workers"]
        self.ray_num_cpus = self.params["ray_num_cpus"]

    def _initialize_store_params(self):

        store_params = self.params[store_params_key]

        # update s3_creds, needed for store_type_s3
        store_params = store_params | {"s3_creds": self.s3_cred}
        self.params[store_params_key] = self.params[store_params_key] | init_store_params(store_params, self.logger)
        self.store_backend_dir = None

        return True

    def get_transform_config(
        self,
        data_access_factory: DataAccessFactoryBase,
        statistics: ActorHandle,
        files: list[str],
    ) -> dict[str, Any]:
        """
        Set environment for transform execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of transform init params
        """
        self.logger.info("=> get_transform_config started")
        self.store_backend_dir = self.params[store_dir_key]
        data_access = data_access_factory.create_data_access()
        self.input_folder = data_access.input_folder
        self.output_folder = data_access.output_folder
        # Keep s3_creds
        self.daf = data_access_factory
        self.s3_cred = data_access_factory.s3_cred
        self.data_access = data_access
        self._initialize_store_params()
        self.store_params = self.params[store_params_key]
        self.logger.info("<= get_transform_config")
        self.start_time = datetime.datetime.now()
        return self.params

    def _prepare_mapper_function(self):
        # Prepare mapper function according to cli args
        # this function wraps the mappings to be done on a table
        # here whether we want sorting or superrows or even
        # want output folders arranged by language
        mapper_function_params = {}

        from dpk_repo_level_order.internal.repo_level_wrappers import (
            dominant_lang_per_repo,
            get_transforming_func,
        )

        if self.sorting_enabled:
            self.logger.info(f"Repo level sorting is enabled. Algo: {self.sorting_algo}")
            from dpk_repo_level_order.internal.repo_level_wrappers import (
                SORT_BY_PATH,
                SORT_SEMANTIC,
                SORT_SEMANTIC_NORMALISED,
                get_sorting_func,
            )

            sort_func = get_sorting_func(self.sorting_algo, "title", self.logger)
            # Add sort_func to params
            mapper_function_params = mapper_function_params | {
                "sorting_func": sort_func,
            }

        if self.output_by_langs:
            mapper_function_params = mapper_function_params | {
                "filename_func": dominant_lang_per_repo,
            }

        if self.combine_rows:
            from dpk_repo_level_order.internal.repo_level_wrappers import superrow_table

            mapper_function_params = mapper_function_params | {"superrows_func": superrow_table}

        repo_mapper_func = get_transforming_func(**mapper_function_params)
        return repo_mapper_func

    def _group_and_sort(self):

        self.logger.info(f"Stage 1 Finished in {datetime.datetime.now() - self.start_time}.")

        from dpk_repo_level_order.internal.repo_grouper import GroupByRepoActor
        from ray.util import ActorPool

        store = create_store(self.store_params)
        # store = FSStore(self.store_backend_dir)
        # We need input path here
        files_location = self.input_folder
        output_location = self.output_folder
        # If I normalised values in store, then there is no need of this generating full path. TODO
        p_input = []
        for repo in store.items():
            p_input.append((repo, list(map(lambda x: f"{files_location}/{x}", store.get(repo)))))

        self.logger.info(f"Processing {len(p_input)} repos with {self.ray_workers} workers")
        if self.stage_one_only:
            return {"nrepos": len(p_input)}

        repo_mapper_func = self._prepare_mapper_function()
        processors = RayUtils.create_actors(
            clazz=GroupByRepoActor,
            params={
                "repo_column_name": "repo_name",
                "output_dir": output_location,
                "logger": None,  # may be this logger causes the arguments to be non serializable
                "data_access_creds": self.s3_cred,
                "data_access_factory": self.daf,
                "mapper": repo_mapper_func,
            },
            actor_options={"num_cpus": self.ray_num_cpus},
            n_actors=self.ray_workers,
        )

        p_pool = ActorPool(processors)
        replies = list(p_pool.map_unordered(lambda a, x: a.process.remote(x[0], x[1]), p_input))
        return {"nrepos": len(p_input)}

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        # Get filters stats
        self.logger.info(f"Store Backend is {self.store_backend_dir}")
        second_stage_stats = self._group_and_sort()
        self.logger.info(f"Finished the transform in {datetime.datetime.now() - self.start_time} ")
        return stats | second_stage_stats


class RepoLevelOrderTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=RepoLevelOrderTransform,
            remove_from_metadata=[store_params_key],
        )

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the RepoLevelOrderTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, repo_level_order_, pii_, etc.)
        """
        # An example of a command line option that we don't want included
        # in the metadata collected by the Ray orchestrator
        # See below for remove_from_metadata addition so that it is not reported.
        parser.add_argument(
            f"--{cli_prefix}stage_one_only",
            action="store_true",
            help="If this flag is set, transform only builds the repo grouping and doesn't write output",
        )
        parser.add_argument(
            f"--{cli_prefix}{grouping_column_key}",
            type=str,
            default="repo_name",
            help="The name of the column which has repo name.",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_type_key}",
            type=str,
            default="ray",
            help="Intermediate store to hold repo grouping info. Should be one of (ray, s3, local). s3 and local are persistent, ray is ephemeral",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_dir_key}",
            type=str,
            help="Backend dir for store, if store is of type local or s3.",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_ray_cpus_key}",
            type=float,
            default=0.5,
            help="Needed for store type ray",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_ray_nworkers_key}",
            type=int,
            default=1,
            help="Needed for store type ray. Number of workers.",
        )
        parser.add_argument(
            f"--{cli_prefix}{sorting_enable_key}",
            action="store_true",
            help="Enables sorting of output.",
        )
        parser.add_argument(
            f"--{cli_prefix}{sorting_algo_key}",
            type=str,
            default="SORT_BY_PATH",
            help="Specifies sorting algo. It is one of SORT_SEMANTIC, SORT_BY_PATH, SORT_SEMANTIC_NORMALISED",
        )
        parser.add_argument(
            f"--{cli_prefix}{output_by_langs_key}",
            action="store_true",
            help="If specified, output is grouped into programming language folders.",
        )
        parser.add_argument(
            f"--{cli_prefix}{output_superrows_key}",
            action="store_true",
            help="If specified, output rows per repo are combined to form a single repo",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        store_params = create_store_params(self.params)
        self.logger.debug(f"store params in config: {store_params.keys()}")
        self.params = self.params | {grouping_column_key: self.params[grouping_column_key]} | store_params

        runtime_captured = CLIArgumentProvider.capture_parameters(args, "runtime_", False)
        ray_actor_params = {
            "ray_workers": runtime_captured["num_workers"],
            "ray_num_cpus": runtime_captured["worker_options"]["num_cpus"],
        }
        self.params = self.params | ray_actor_params
        return True


class RepoLevelOrderRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=RepoLevelOrderTransformConfiguration(),
            runtime_class=RepoLevelOrderRuntime,
        )
