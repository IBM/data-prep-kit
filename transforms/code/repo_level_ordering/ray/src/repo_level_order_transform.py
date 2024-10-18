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
from data_processing.data_access import DataAccessFactoryBase
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, get_logger, str2bool
from data_processing_ray.runtime.ray import DefaultRayTransformRuntime, RayUtils
from data_processing_ray.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from dpk_repo_level_order.internal.store.store_factory import (
    create_store,
    create_store_params,
    init_store_params,
    store_type_value_ray,
    validate_store_params,
)
from ray.actor import ActorHandle


short_name = "repo_lvl"
cli_prefix = f"{short_name}_"

repo_column_default_value = "repo_name"
grouping_column_key = "grouping_column"
language_column_key = "language_column"
store_params_key = "store_params"

store_type_key = "store_type"
store_dir_key = "store_backend_dir"
store_ray_cpus_key = "store_ray_cpus"
store_ray_nworkers_key = "store_ray_nworkers"

stage_one_only_key = "stage_one_only"
stage_two_ray_workers_key = "ray_workers"
stage_two_ray_cpus_key = "ray_num_cpus"


sorting_enable_key = "sorting_enabled"
sorting_algo_key = "sorting_algo"

output_by_langs_key = "output_by_langs"
output_superrows_key = "combine_rows"

group_batch_size = 50

# default cli args
language_column_default_value = "language"
rworkers_default = 1
rcpus_default = 0.5
store_type_default = "ray"
sort_enable_default = False
sort_algo_default = "SORT_BY_PATH"
output_by_lang_default = False
superrows_default = False


class RepoLevelOrderTransform(AbstractTableTransform):
    """
    Prepares a list of groups in the file and add them to store.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        The dictionary should contain the following:
            grouping_column - name of the column to do groupby
            store_params - A dictionary to create a key-value store which stores key: str -> value: List(str)
                           The dictionary should contain:
                           store_type - Type of store. One of [store_type_value_s3, store_type_value_local, store_type_value_ray ].
                                        store_type_value_ray is preferred for cluster,
                                        store_type_value_local for single node if cpus are less.
                           store_backend_dir - A path for
                                        store types [ store_type_value_s3, store_type_value_local]
                           s3_creds: A dictionary for S3 creds if using store_type_value_s3.
                                     {'access_key', 'secret_key', 'url'},
                                     None if store is ray or local
                           store_pool_key: A list of actors if store_type_value_ray, otherwise None
                           store_ray_cpus_key: Num of cpus used, if store_type_value_ray
                           store_ray_nworkers_key: Number of workers , used if store_type_value_ray

                           These params are created/updated and validated via
                            `dpk_repo_level_order.internal.store.store_factory` module.

        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of RepoLevelOrderTransformConfiguration class
        super().__init__(config)

        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)
        self.config = config
        self.grouping_column = config.get(grouping_column_key, repo_column_default_value)
        store_params = config.get(store_params_key)
        validate_store_params(store_params)
        self.store_type = store_params[store_type_key]
        self.store = create_store(store_params)
        self.group_batch_size = group_batch_size

    def _create_batches(self, data, batch_size=1):
        batch = []
        batches = []
        iterator = iter(data)
        try:
            while True:
                for _ in range(batch_size):
                    batch.append(next(iterator))
                batches.append(batch)
                batch = []
        except StopIteration:
            if batch:
                batches.append(batch)
        return batches

    def _normalize_file_name_for_store(self, file_name):
        if self.store_type == store_type_value_ray:
            # we can store full file_name consiting of full path in this store.
            return file_name
        else:
            # since this store type uses filesystem as backend
            # can't store full path in store since,
            # store is currently flat filesystem.
            return os.path.basename(file_name)

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        This step is used to do groupby with respect to `self.grouping_column` and update
        the group and file_name to a store referenced by self.store.
        """
        self.logger.debug(f"Transforming one table with {len(table)} rows")
        grouped = table.group_by(self.grouping_column)
        repo_groups = grouped.aggregate([(self.grouping_column, "count")])[self.grouping_column].to_pylist()

        batch_size = self.group_batch_size
        if len(repo_groups) < batch_size:
            batch_size = len(repo_groups)

        batches = self._create_batches(repo_groups, batch_size)

        for batch in batches:
            grp_flow = {}
            for group in batch:
                # This supports only flat folder structure, so all
                # files should be in the same folder.
                file_name = self._normalize_file_name_for_store(file_name)
                grp_flow[group] = file_name
                self.logger.debug(f"Updating {group} to store")

            self.store.put_dict(grp_flow)

        stats = {"identified_groups": len(repo_groups)}
        self.logger.debug(f"Transformed one table with {len(table)} rows")
        metadata = {"nfiles": 1, "nrows": len(table)} | stats
        return [], metadata


class RepoLevelOrderRuntime(DefaultRayTransformRuntime):
    """
    RepoLevelOrder runtime support.
    This runtime creates and initialises a store which can store
    information as a key/value dict like structure
    { groups: list of files where group is found}. The transform
    populates the store and the `compute_execution_stats` method of this
    runtime reads the store to do actually read the files
    per group and apply the selected transformations (sorting/combining rows)
    and writes a parquet file per group.
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            sorting_enabled - True if sorting is enabled, it is an optional parameter.
            sorting_algo_key - One of [SORT_BY_PATH, SORT_SEMANTIC, SORT_SEMANTIC_NORMALISED, None]
            ray_workers - Number of ray workers to grouped and process data
            ray_num_cpus - Number of cpus per Ray Actor.
            output_by_langs_key - Bool, if True, the output files are written to language folder
            output_superrows_key - Bool, if True, all rows in output are collated toa single row.
            store_params - A dictionary to create/update store, used by transform and runtime.

        """
        self.logger = get_logger(__name__)
        super().__init__(params)

        self.stage_one_only = self.params[stage_one_only_key]
        self.sorting_enabled = self.params[sorting_enable_key]
        self.sorting_algo = self.params[sorting_algo_key]

        self.output_by_langs = self.params[output_by_langs_key]
        self.combine_rows = self.params[output_superrows_key]
        self.ray_workers = self.params[stage_two_ray_workers_key]
        self.ray_num_cpus = self.params[stage_two_ray_cpus_key]

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
        self.repo_column_name = self.params[grouping_column_key]
        self.language_column_name = self.params[language_column_key]
        return self.params

    def _prepare_mapper_function(self):
        # Prepare mapper function according to cli args
        # this function wraps the mappings to be done on a table
        # here whether we want sorting or superrows or even
        # want output folders arranged by language
        mapper_function_params = {}

        from dpk_repo_level_order.internal.repo_level_wrappers import (
            get_dominant_language_func,
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

            sort_func = get_sorting_func(self.sorting_algo, "title", self.logger, self.language_column_name)
            # Add sort_func to params
            mapper_function_params = mapper_function_params | {
                "sorting_func": sort_func,
            }

        if self.output_by_langs:
            self.logger.info("Output by language enabled.")
            mapper_function_params = mapper_function_params | {
                "filename_func": get_dominant_language_func(
                    language_column_name=self.language_column_name, title_column_name="title"
                ),
            }

        if self.combine_rows:
            self.logger.info("Combine rows enabled.")
            from dpk_repo_level_order.internal.repo_level_wrappers import superrow_table

            mapper_function_params = mapper_function_params | {"superrows_func": superrow_table}

        mapper_function_params = mapper_function_params | {"language_column_name": self.language_column_name}
        repo_mapper_func = get_transforming_func(**mapper_function_params)
        return repo_mapper_func

    def _prepare_inputs(self):
        store = create_store(self.store_params)
        store_type = self.store_params[store_type_key]

        p_input = []
        for repo, files in store.items_kv():
            if store_type == store_type_value_ray:
                p_input.append((repo, [f"{file}" for file in files]))
            else:
                files_location = self.input_folder
                p_input.append((repo, [f"{files_location}/{file}" for file in files]))
        return p_input

    def _group_and_sort(self):

        self.logger.info(f"Stage 1 Finished in {datetime.datetime.now() - self.start_time}.")

        from dpk_repo_level_order.internal.repo_grouper import GroupByRepoActor
        from ray.util import ActorPool

        p_input = self._prepare_inputs()
        if self.stage_one_only:
            return {"nrepos": len(p_input)}

        repo_mapper_func = self._prepare_mapper_function()
        processors = RayUtils.create_actors(
            clazz=GroupByRepoActor,
            params={
                "repo_column_name": self.repo_column_name,
                "output_dir": self.output_folder,
                "data_access_factory": self.daf,
                "mapper": repo_mapper_func,
            },
            actor_options={"num_cpus": self.ray_num_cpus},
            n_actors=self.ray_workers,
        )

        p_pool = ActorPool(processors)
        self.logger.info(f"Processing {len(p_input)} repos with {self.ray_workers} workers")
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
            f"--{cli_prefix}{stage_one_only_key}",
            type=lambda x: bool(str2bool(x)),
            help="If this flag is True, transform only builds the repo grouping and doesn't write output",
        )
        parser.add_argument(
            f"--{cli_prefix}{grouping_column_key}",
            type=str,
            default=repo_column_default_value,
            help="The name of the column which has repo name.",
        )
        parser.add_argument(
            f"--{cli_prefix}{language_column_key}",
            type=str,
            default=language_column_default_value,
            help="The name of the column which has programming language name.",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_type_key}",
            type=str,
            default=store_type_default,
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
            default=rcpus_default,
            help="Needed for store type ray",
        )
        parser.add_argument(
            f"--{cli_prefix}{store_ray_nworkers_key}",
            type=int,
            default=rworkers_default,
            help="Needed for store type ray. Number of workers.",
        )
        parser.add_argument(
            f"--{cli_prefix}{sorting_enable_key}",
            default=sort_enable_default,
            type=lambda x: bool(str2bool(x)),
            help=f"Enables sorting of output by algorithm specified using {cli_prefix}{sorting_algo_key}. Defaults to SORT_BY_PATH if no algorithm is specified.",
        )
        parser.add_argument(
            f"--{cli_prefix}{sorting_algo_key}",
            type=str,
            default=sort_algo_default,
            help="Specifies sorting algo. It is one of SORT_SEMANTIC, SORT_BY_PATH, SORT_SEMANTIC_NORMALISED",
        )
        parser.add_argument(
            f"--{cli_prefix}{output_by_langs_key}",
            type=lambda x: bool(str2bool(x)),
            default=output_by_lang_default,
            help="If specified, output is grouped into programming language folders.",
        )
        parser.add_argument(
            f"--{cli_prefix}{output_superrows_key}",
            type=lambda x: bool(str2bool(x)),
            default=superrows_default,
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
            stage_two_ray_workers_key: runtime_captured["num_workers"],
            stage_two_ray_cpus_key: runtime_captured["worker_options"]["num_cpus"],
        }
        self.params = self.params | ray_actor_params
        return True


class RepoLevelOrderRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(
            transform_config=RepoLevelOrderTransformConfiguration(),
            runtime_class=RepoLevelOrderRuntime,
        )
