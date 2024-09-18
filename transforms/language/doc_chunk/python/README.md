# Chunk documents Transform 

This transform is chunking documents. It supports multiple _chunker modules_ (see the `chunking_type` parameter).

When using documents converted to JSON, the transform leverages the [Docling Core](https://github.com/DS4SD/docling-core) `HierarchicalChunker`
to chunk according to the document layout segmentation, i.e. respecting the original document components as paragraphs, tables, enumerations, etc.
It relies on documents converted with the Docling library in the [pdf2parquet transform](../pdf2parquet) using the option `contents_type: "application/json"`,
which provides the required JSON structure.

When using documents converted to Markdown, the transform leverages the [Llama Index](https://docs.llamaindex.ai/en/stable/module_guides/loading/node_parsers/modules/#markdownnodeparser) `MarkdownNodeParser`, which is relying on its internal Markdown splitting logic.

## Output format

The output parquet file will contain all the original columns, but the content will be replaced with the individual chunks.


### Tracing the origin of the chunks

The transform allows to trace the origin of the chunk with the `source_doc_id` which is set to the value of the `document_id` column (if present) in the input table.
The actual name of columns can be customized with the parameters described below.


## Running

### Parameters

The transform can be tuned with the following parameters.


| Parameter  | Default  | Description  |
|------------|----------|--------------|
| `chunking_type`        | `dl_json` | Chunking type to apply. Valid options are `li_markdown` for using the LlamaIndex [Markdown chunking](https://docs.llamaindex.ai/en/stable/module_guides/loading/node_parsers/modules/#markdownnodeparser), `dl_json` for using the [Docling JSON chunking](https://github.com/DS4SD/docling). |
| `content_column_name`        | `contents` | Name of the column containing the text to be chunked. |
| `doc_id_column_name`         | `document_id` | Name of the column containing the doc_id to be propagated in the output. |
| `dl_min_chunk_len`           | `None` | Minimum number of characters for the chunk in the dl_json chunker. Setting to None is using the library defaults, i.e. a `min_chunk_len=64`. |
| `output_chunk_column_name`   | `contents` | Column name to store the chunks in the output table. |
| `output_source_doc_id_column_name`   | `source_document_id` | Column name to store the `doc_id` from the input table. |
| `output_jsonpath_column_name`| `doc_jsonpath` | Column name to store the document path of the chunk in the output table. |
| `output_pageno_column_name`  | `page_number` | Column name to store the page number of the chunk in the output table. |
| `output_bbox_column_name`    | `bbox` | Column name to store the bbox of the chunk in the output table. |

When invoking the CLI, the parameters must be set as `--doc_chunk_<name>`, e.g. `--doc_chunk_column_name_key=myoutput`.


### Running the samples
To run the samples, use the following `make` targets

* `run-cli-sample` - runs src/doc_chunk_transform.py using command line args
* `run-local-sample` - runs src/doc_chunk_local.py

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 
```shell
make run-cli-sample
...
```
Then 
```shell
ls output
```
To see results of the transform.

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
