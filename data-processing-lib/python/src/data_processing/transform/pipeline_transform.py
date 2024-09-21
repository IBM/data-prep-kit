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

from typing import Any
from data_processing.transform import AbstractBinaryTransform, BaseTransformRuntime
from data_processing.utils import get_logger, UnrecoverableException, TransformUtils


class AbstractPipelineTransform(AbstractBinaryTransform):
    """
    Transform that executes a set of base transforms sequentially. Data is passed between
    participating transforms in memory
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initializes pipeline execution for the list of transforms
        :param config - configuration parameters - dictionary of transforms in the pipeline.
        Note that transforms will be executed
        be executed
        """
        super().__init__({})
        self.logger = get_logger(__name__)
        transforms = config.get("transforms", [])
        if len(transforms) == 0:
            # Empty pipeline
            self.logger.error("Pipeline transform with empty list")
            raise UnrecoverableException("Pipeline transform with empty list")
        self.data_access_factory = config.get("data_access_factory", None)
        if self.data_access_factory is None:
            self.logger.error("pipeline transform - Data access factory is not defined")
            raise UnrecoverableException("pipeline transform - Data access factory is not defined")
        self.statistics = config.get("statistics", None)
        if self.statistics is None:
            self.logger.error("pipeline transform - Statistics is not defined")
            raise UnrecoverableException("pipeline transform - Statistics is not defined")
        self.transforms = transforms
        participants = []
        # for every transform in the pipeline
        for transform in transforms:
            # create runtime
            runtime = transform.create_transform_runtime()
            # get parameters
            transform_params = self._get_transform_params(runtime)
            # Create transform
            tr = transform.get_transform_class()(transform_params)
            participants.append((tr, runtime))
        # save participating transforms
        self.participants = participants

    def _get_transform_params(self, runtime: BaseTransformRuntime) -> dict[str, Any]:
        """
        get transform parameters
        :param runtime - runtime
        :return: transform params
        """
        raise NotImplemented("must be implemented by subclass")

    def transform_binary(self, file_name: str, byte_array: bytes) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Converts input file into o or more output files.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :param byte_array: contents of the input file to be transformed.
        :param file_name: the name of the file containing the given byte_array.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        # process transforms sequentially
        data = [(byte_array, file_name)]
        stats = {}
        for transform, _ in self.participants:
            data, st = self._process_transform(transform=transform, data=data)
            # Accumulate stats
            stats |= st
            if len(data) == 0:
                # no data returned by this transform
                return [], stats
        # all done
        return self._convert_output(data), stats

    @staticmethod
    def _convert_output(data: list[tuple[bytes, str]]) -> list[tuple[bytes, str]]:
        res = [None] * len(data)
        i = 0
        for dt in data:
            fname = TransformUtils.get_file_extension(dt[1])
            ext = fname[1]
            if len(ext) <= 1:
                ext = fname[0]
            res[i] = (dt[0], ext)
            i += 1
        return res

    @staticmethod
    def _process_transform(transform: AbstractBinaryTransform, data: list[tuple[bytes, str]]
                           ) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        Process individual transform. Note here that the predecessor could create multiple data objects
        :param transform - transform
        :param data - data to process
        :return:
        """
        stats = {}
        res = []
        for dt in data:
            # for every data element
            src = TransformUtils.get_file_extension(dt[1])
            out_files, st = transform.transform_binary(byte_array=dt[0], file_name=dt[1])
            # Accumulate results
            for ouf in out_files:
                res.append((ouf[0], src[0] + ouf[1]))
            # accumulate statistics
            stats |= st
        return res, stats

    def flush_binary(self) -> tuple[list[tuple[bytes, str]], dict[str, Any]]:
        """
        This is supporting method for transformers, that implement buffering of data, for example coalesce.
        These transformers can have buffers containing data that were not written to the output immediately.
        Flush is the hook for them to return back locally stored data and their statistics.
        The majority of transformers are expected not to use such buffering and can use this default implementation.
        If there is an error, an exception must be raised - exit()ing is not generally allowed.
        :return: a tuple of a list of 0 or more tuples and a dictionary of statistics that will be propagated
                to metadata.  Each element of the return list, is a tuple of the transformed bytes and a string
                holding the extension to be used when writing out the new bytes.
        """
        stats = {}
        res = []
        i = 0
        for transform, _ in self.participants:
            out_files, st = transform.flush_binary()
            # accumulate statistics
            stats |= st
            if len(out_files) > 0 and i < len(self.participants) - 1:
                # flush produced output - run it through the rest of the chain
                data = []
                for ouf in out_files:
                    data.append((ouf[0], f"file{ouf[1]}"))
                for n in range(i + 1, len(self.participants)):
                    data, st = self._process_transform(transform=self.participants[n][0], data=data)
                    # Accumulate stats
                    stats |= st
                    if len(data) == 0:
                        # no data returned by this transform
                        break
                    res += self._convert_output(data)
            else:
                res += self._convert_output(out_files)
            i += 1
        # Done flushing, compute execution stats
        for _, runtime in self.participants:
            self._compute_execution_stats(runtime=runtime, st=stats)
        return res, {}

    def _compute_execution_stats(self, runtime: BaseTransformRuntime, st: dict[str, Any]) -> None:
        """
        get transform parameters
        :param runtime - runtime
        :param st - statistics
        :return: None
        """
        raise NotImplemented("must be implemented by subclass")
