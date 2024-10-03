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

import enum
import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils, get_logger
from doc_chunk_chunkers import ChunkingExecutor, DLJsonChunker, LIMarkdown, LITokenTextSplitter


short_name = "doc_chunk"
cli_prefix = f"{short_name}_"
content_column_name_key = "content_column_name"
doc_id_column_name_key = "doc_id_column_name"
chunking_type_key = "chunking_type"
dl_min_chunk_len_key = "dl_min_chunk_len"
chunk_size_tokens_key = "chunk_size_tokens"
chunk_overlap_tokens_key = "chunk_overlap_tokens"
output_chunk_column_name_key = "output_chunk_column_name"
output_chunk_column_id_key = "output_chunk_column_id"
output_source_doc_id_column_name_key = "output_source_doc_id_column_name"
output_jsonpath_column_name_key = "output_jsonpath_column_name"
output_pageno_column_name_key = "output_pageno_column_name"
output_bbox_column_name_key = "output_bbox_column_name"
content_column_name_cli_param = f"{cli_prefix}{content_column_name_key}"
doc_id_column_name_cli_param = f"{cli_prefix}{doc_id_column_name_key}"
chunking_type_cli_param = f"{cli_prefix}{chunking_type_key}"
dl_min_chunk_len_cli_param = f"{cli_prefix}{dl_min_chunk_len_key}"
output_chunk_column_name_cli_param = f"{cli_prefix}{output_chunk_column_name_key}"
output_source_doc_id_column_name_cli_param = f"{cli_prefix}{output_source_doc_id_column_name_key}"
output_jsonpath_column_name_cli_param = f"{cli_prefix}{output_jsonpath_column_name_key}"
output_pageno_column_name_cli_param = f"{cli_prefix}{output_pageno_column_name_key}"
output_bbox_column_name_cli_param = f"{cli_prefix}{output_bbox_column_name_key}"
chunk_size_tokens_cli_param = f"{cli_prefix}{chunk_size_tokens_key}"
chunk_overlap_tokens_cli_param = f"{cli_prefix}{chunk_overlap_tokens_key}"

class chunking_types(str, enum.Enum):
    LI_MARKDOWN = "li_markdown"
    DL_JSON = "dl_json"
    LI_TOKEN_TEXT = "li_token_text"

    def __str__(self):
        return str(self.value)


default_content_column_name = "contents"
default_doc_id_column_name = "document_id"
default_chunking_type = chunking_types.DL_JSON
default_dl_min_chunk_len = None
default_output_chunk_column_name = "contents"
default_output_chunk_column_id = "chunk_id"
default_output_source_doc_id_column_name = "source_document_id"
default_output_jsonpath_column_name = "doc_jsonpath"
default_output_pageno_column_name = "page_number"
default_output_bbox_column_name = "bbox"
default_chunk_size_tokens = 128
default_chunk_overlap_tokens = 30

class DocChunkTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, DocChunkTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of DocChunkTransformConfiguration class
        super().__init__(config)
        self.logger = get_logger(__name__)

        self.chunking_type = config.get(chunking_type_key, default_chunking_type)

        self.content_column_name = config.get(content_column_name_key, default_content_column_name)
        self.doc_id_column_name = config.get(doc_id_column_name_key, default_doc_id_column_name)
        self.output_chunk_column_name = config.get(output_chunk_column_name_key, default_output_chunk_column_name)
        self.output_chunk_column_id = config.get(output_chunk_column_id_key, default_output_chunk_column_id)
        self.output_source_doc_id_column_name = config.get(output_source_doc_id_column_name_key, default_output_source_doc_id_column_name)

        # Parameters for Docling JSON chunking
        self.dl_min_chunk_len = config.get(dl_min_chunk_len_key, default_dl_min_chunk_len)
        self.output_jsonpath_column_name = config.get(
            output_jsonpath_column_name_key, default_output_jsonpath_column_name
        )
        self.output_pageno_column_name_key = config.get(
            output_pageno_column_name_key, default_output_pageno_column_name
        )
        self.output_bbox_column_name_key = config.get(output_bbox_column_name_key, default_output_bbox_column_name)

        # Parameters for Fixed-size with overlap chunking 
        self.chunk_size_tokens = config.get(chunk_size_tokens_key, default_chunk_size_tokens)
        self.chunk_overlap_tokens = config.get(chunk_overlap_tokens_key, default_chunk_overlap_tokens)

        # Initialize chunker

        self.chunker: ChunkingExecutor
        if self.chunking_type == chunking_types.DL_JSON:
            self.chunker = DLJsonChunker(
                min_chunk_len=self.dl_min_chunk_len,
                output_chunk_column_name=self.output_chunk_column_name,
                output_jsonpath_column_name=self.output_jsonpath_column_name,
                output_pageno_column_name_key=self.output_pageno_column_name_key,
                output_bbox_column_name_key=self.output_bbox_column_name_key,
            )
        elif self.chunking_type == chunking_types.LI_MARKDOWN:
            self.chunker = LIMarkdown(
                output_chunk_column_name=self.output_chunk_column_name,
            )
        elif self.chunking_type == chunking_types.LI_TOKEN_TEXT:
            self.chunker = LITokenTextSplitter(
                output_chunk_column_name=self.output_chunk_column_name,
                output_chunk_column_id=self.output_chunk_column_id,
                chunk_size_tokens=self.chunk_size_tokens,
                chunk_overlap_tokens=self.chunk_overlap_tokens
            )
        else:
            raise RuntimeError(f"{self.chunking_type=} is not valid.")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """ """
        self.logger.debug(f"Transforming one table with {len(table)} rows")

        # make sure that the content column exists
        TransformUtils.validate_columns(table=table, required=[self.content_column_name])

        data = []
        for batch in table.to_batches():
            for row in batch.to_pylist():
                content: str = row[self.content_column_name]
                new_row = {k: v for k, v in row.items() if k not in (self.content_column_name, self.doc_id_column_name)}
                if self.doc_id_column_name in row:
                    new_row[self.output_source_doc_id_column_name] = row[self.doc_id_column_name]
                for chunk in self.chunker.chunk(content):
                    chunk[self.doc_id_column_name] = TransformUtils.str_to_hash(chunk[self.output_chunk_column_name])
                    data.append(
                        {
                            **new_row,
                            **chunk,
                        }
                    )

        table = pa.Table.from_pylist(data)
        metadata = {
            "nfiles": 1,
            "nrows": len(table),
        }
        return [table], metadata


class DocChunkTransformConfiguration(TransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args.
    """

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=DocChunkTransform,
        )

        self.logger = get_logger(__name__ + "cfg")  # workaround issue #481

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given  parser.
        This will be included in a dictionary used to initialize the DocChunkTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, DocChunk_, pii_, etc.)
        """
        parser.add_argument(
            f"--{chunking_type_cli_param}",
            default=default_chunking_type,
            choices=list(chunking_types),
            help="Chunking type to apply. Valid options are li_markdown for using the LlamaIndex Markdown chunking, dl_json for using the Docling JSON chunking.",
        )
        parser.add_argument(
            f"--{content_column_name_cli_param}",
            default=default_content_column_name,
            help="Name of the column containing the text to be chunked",
        )
        parser.add_argument(
            f"--{doc_id_column_name_cli_param}",
            default=default_doc_id_column_name,
            help="Name of the column containing the doc_id to be propagated in the output",
        )
        parser.add_argument(
            f"--{dl_min_chunk_len_cli_param}",
            default=default_dl_min_chunk_len,
            help="Minimum number of characters for the chunk in the dl_json chunker. Setting to None is using the library defaults, i.e. a min_chunk_len=64.",
        )
        parser.add_argument(
            f"--{output_chunk_column_name_cli_param}",
            default=default_output_chunk_column_name,
            help="Column name to store the chunks",
        )
        parser.add_argument(
            f"--{output_source_doc_id_column_name_cli_param}",
            default=default_output_source_doc_id_column_name,
            help="Column name to store the `document_id` from the input table",
        )
        parser.add_argument(
            f"--{output_jsonpath_column_name_cli_param}",
            default=default_output_jsonpath_column_name,
            help="Column name to store the document path of the chunk",
        )
        parser.add_argument(
            f"--{output_pageno_column_name_cli_param}",
            default=default_output_pageno_column_name,
            help="Column name to store the page number of the chunk",
        )
        parser.add_argument(
            f"--{output_bbox_column_name_cli_param}",
            default=default_output_bbox_column_name,
            help="Column name to store the bbox of the chunk",
        )
        parser.add_argument(
            f"--{chunk_size_tokens_cli_param}",
            default=default_chunk_size_tokens,
            type=int,
            help="Size of the chunk in tokens for the fixed-sized chunker",
        )
        parser.add_argument(
            f"--{chunk_overlap_tokens_cli_param}",
            default=default_chunk_overlap_tokens,
            type=int,
            help="Number of tokens overlapping between chunks for the fixed-sized chunker.",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)

        self.params = self.params | captured
        self.logger.info(f"doc_chunk parameters are : {self.params}")
        return True
