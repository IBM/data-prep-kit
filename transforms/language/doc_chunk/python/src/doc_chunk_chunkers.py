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

from abc import ABCMeta, abstractmethod
from typing import Iterator, Optional

from docling_core.types import Document as DLDocument
from llama_index.core import Document as LIDocument
from llama_index.core.node_parser import MarkdownNodeParser
from docling_core.transforms.chunker import HierarchicalChunker


class ChunkingExecutor(metaclass=ABCMeta):
    @abstractmethod
    def chunk(self, content: str) -> Iterator[dict]:
        raise NotImplemented("The chunk() method must be implemented")


class DLJsonChunker(ChunkingExecutor):
    def __init__(
        self,
        min_chunk_len: Optional[int],
        output_chunk_column_name: str,
        output_jsonpath_column_name: str,
        output_pageno_column_name_key: str,
        output_bbox_column_name_key: str,
    ):
        self.output_chunk_column_name = output_chunk_column_name
        self.output_jsonpath_column_name = output_jsonpath_column_name
        self.output_pageno_column_name_key = output_pageno_column_name_key
        self.output_bbox_column_name_key = output_bbox_column_name_key

        chunker_kwargs = dict(include_metadata=True)
        if min_chunk_len is not None:
            chunker_kwargs["min_chunk_len"] = min_chunk_len
        self._chunker = HierarchicalChunker(**chunker_kwargs)

    def chunk(self, content: str) -> Iterator[dict]:
        doc = DLDocument.model_validate_json(content)
        for chunk in self._chunker.chunk(doc):
            yield {
                self.output_chunk_column_name: chunk.text,
                self.output_jsonpath_column_name: chunk.path,
                self.output_pageno_column_name_key: chunk.page,
                self.output_bbox_column_name_key: chunk.bbox,
            }


class LIMarkdown(ChunkingExecutor):
    def __init__(self, output_chunk_column_name: str):
        self.output_chunk_column_name = output_chunk_column_name
        self._chunker = MarkdownNodeParser()

    def chunk(self, content: str) -> Iterator[dict]:
        doc = LIDocument(text=content, mimetype="text/markdown")
        for node in self._chunker.get_nodes_from_documents(documents=[doc]):
            yield {
                self.output_chunk_column_name: node.text,
            }
