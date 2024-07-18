# PDF2MD Ray Transform 

This module implements the ray version of the [pdf2md transform](../python/).


## Prometheus metrics

The transform will produce the following statsd metrics:

| metric name                      | Description                                                      |
|----------------------------------|------------------------------------------------------------------|
| worker_pdf_doc_count             | Number of PDF documents converted by the worker                  |
| worker_pdf_pages_count           | Number of PDF pages converted by the worker                      |
| worker_pdf_page_avg_convert_time | Average time for converting a single PDF page on each worker     |
| worker_pdf_convert_time          | Time spent converting a single document                          |

