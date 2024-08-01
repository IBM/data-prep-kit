# Ingest PDF to Parquet

This tranforms iterate through PDF files or zip of PDF files and generates parquet files
containing the converted document in Markdown format.

The PDF conversion is using the [Docling package](https://github.com/DS4SD/docling).


## Output format

The output format will contain all the columns of the metadata CSV file,
with the addition of the following columns

```jsonc
{
    "source_filename": "string",  // the basename of the source archive or file
    "filename": "string",         // the basename of the PDF file
    "contents": "string",         // the content of the PDF
    "document_id": "string",      // the document id, a random uuid4 
    "ext": "string",              // the detected file extension
    "hash": "string",             // the hash of the `contents` column
    "size": "string",             // the size of `contents`
    "date_acquired": "date",      // the date when the transform was executing
    "num_pages": "number",        // number of pages in the PDF
    "num_tables": "number",       // number of tables in the PDF
    "num_doc_elements": "number", // number of document elements in the PDF
    "pdf_convert_time": "float",  // time taken to convert the document in seconds
}
```


## Parameters

The transform can be initialized with the following parameters.

| Parameter  | Default  | Description  |
|------------|----------|--------------|
| `artfacts_path`              | <unset> | Path where to models artifacts are located, if unset they will be downloaded in the HF_CACHE folder. |
| `do_table_structure`         | `True`        | If true, detected tables will be processed with the table structure model.                                                                   |
| `do_ocr`                     | `False`        | If true, optical character recognization (OCR) will be used to read the PDF content model.                                                                   |

When invoking the CLI, the parameters must be set as `--pdf2parquet_<name>`, e.g. `--pdf2parquet_do_ocr=true`.


## Credits

The PDF document conversion is developed by the AI for Knowledge group in IBM Research Zurich.
The main package is [Docling](https://github.com/DS4SD/docling).
