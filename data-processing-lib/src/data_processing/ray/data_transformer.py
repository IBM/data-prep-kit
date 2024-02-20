from typing import Any

import pyarrow as pa
from fm_data_processing.cli import *
from fm_data_processing.data_access import *


class AbstractDataTransform:
    """
    Converts input to output table
    Sub-classes must provide the filter() method to provide the conversion of one data frame to another.
    """

    def transform(self, table: pa.Table) -> list[pa.Table]:
        """
        converting input table into an output tablr
        :param table: input table
        :return: a list of converted tables to be written to the output
        """
        pass


class DefaultDataTransformerRuntime:
    """
    Fiter runtime used by processor to to create Mutator specific environment
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters
        """
        self.params = params

    def set_environment(self, data_access: DataAccess) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access - data access class
        :return: dictionary of filter init params
        """
        return self.params

    def compute_execution_stats(self, stats: dict[str, Any]) -> dict[str, Any]:
        """
        Compute execution statistics
        :param stats: output of statistics
        :return: job execution statistics
        """
        return stats


class AbstractDataTransformRuntimeFactory(CLIArgumentProvider):
    """
    Provides support the configuration of a transformer.
    """

    def __init__(
        self, runtime_class: type[DefaultDataTransformerRuntime], transformer_class: type[AbstractDataTransform]
    ):
        """
        Initialization
        :param runtime_class: implementation of the Filter runtime
        :param transformer_class: implementation of the Filter
        :return:
        """
        self.runtime = runtime_class
        self.transformer = transformer_class
        self.params = {}

    def create_transformer_runtime(self) -> DefaultDataTransformerRuntime:
        """
        Create Filter runtime
        :return: fiter runtime object
        """
        return self.runtime(self.params)

    def get_transformer(self) -> type[AbstractDataTransform]:
        """
        Create Mutator runtime
        :return: mutator class
        """
        return self.transformer

    def get_input_params_metadata(self) -> dict[str, Any]:
        """
        get input parameters for job_input_params in metadata
        :return:
        """
        pass
