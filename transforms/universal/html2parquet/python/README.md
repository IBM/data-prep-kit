# html2parquet Transform 

This tranforms iterate through zip of HTML files or single HTML files and generates parquet files containing the converted document in string.

The HTML conversion is using the [Trafilatura](https://trafilatura.readthedocs.io/en/latest/usage-python.html).

## Output format

The output format will contain the following colums

```jsonc
{
	"title": "string"             // the member filename
	"document": "string"          // the base of the source archive
	"contents": "string"          // the content of the HTML
    "document_id": "string",      // the document id, a hash of `contents`
    "size": "string",             // the size of `contents`
    "date_acquired": "date",      // the date when the transform was executing
}
```
## Parameters
