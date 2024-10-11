# html2parquet Ray Transform 

This module implements the ray version of the [html2parquet transform](https://github.com/IBM/data-prep-kit/blob/dev/transforms/language/html2parquet/python/README.md).

The HTML conversion is using the [Trafilatura](https://trafilatura.readthedocs.io/en/latest/usage-python.html).

## Prometheus metrics

The transform will produce the following statsd metrics:

| metric name                      | Description                                                      |
|----------------------------------|------------------------------------------------------------------|
| worker_html_doc_count             | Number of HTML documents converted by the worker                  |
| worker_html_pages_count           | Number of HTML pages converted by the worker                      |
| worker_html_page_avg_convert_time | Average time for converting a single HTML page on each worker     |
| worker_html_convert_time          | Time spent converting a single document                           |
