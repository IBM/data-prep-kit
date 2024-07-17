# Ingest PDF to Parquet

This tranforms iterate through PDF files or zip of PDF files and generates parquet files
containing the converted document in Markdown format.

The PDF conversion is using the [Docling package](https://github.com/DS4SD/docling).


## Output format

The output format will contain all the columns of the metadata CSV file,
with the addition of the following columns

```jsonc
{
    // all the fields of the input CSV + ...

    "source_filename": "string",  // the basename of the source archive or file
    "filename": "string",         // the basename of the PDF file
    "contents": "string",         // the content of the PDF
    "document_id": "string",      // the document id, corresponding to the hash of the content 
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

| Parameter  | Default  | Description  |
|------------|----------|--------------|
| `--pdf2md_modelsdir`                  | `./artifacts` | The location where the models are located or downloaded to |
| `--pdf2md_download_models`            | `False`       | If true, the model artifacts will be downloaded, otherwise they must be already at the path specified in `--ingest_pdf_to_parquet_modelsdir` |
| `--pdf2md_do_table_structure`         | `True`        | If true, detected tables will be processed with the table structure model.                                                                   |
| `--pdf2md_do_ocr`                     | `False`        | If true, optical character recognization (OCR) will be used to read the PDF content model.                                                                   |



## Prometheus metrics

The transform will produce the following statsd metrics:

| metric name                      | Description                                                      |
|----------------------------------|------------------------------------------------------------------|
| worker_pdf_doc_count             | Number of PDF documents converted by the worker                  |
| worker_pdf_pages_count           | Number of PDF pages converted by the worker                      |
| worker_pdf_page_avg_convert_time | Average time for converting a single PDF page on each worker     |
| worker_pdf_convert_time          | Time spent converting a single document                          |


## Credits

The PDF document conversion is developed by the AI for Knowledge group in IBM Research Zurich.
The main package is [Docling](https://github.com/DS4SD/docling).
