# Data Tokenization (TODO: to be furher updated)
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 
The data tokenization transform maps an input table to an output table using a pre-trained tokenizer.
The input table must contain at least two columns, by default named `document_id` and `contents`,
and can be specified through `--tkn_doc_id_column` and `--tkn_doc_content_column` respectively.
The `document_id` should be unique within the dataset (across all rows) and the `contents` stores
its corresponding document content.

A pre-trained tokenizer must be specified through `--tkn_tokenizer_path` parameter 
which can either be to a folder containing the pre-trained tokenizer, or a ready-for-download tokenizer
from Huggingface compatible with `AutoTokenizer` library (also from Huggingface).

The tokenization transform will use the pre-trained tokenizer to tokenize each row in the input table
to each row in the output folder as a sequence of token_ids under the `tokens` column. 
The document id and the token count are respectively stored in the `document_id` (or name specified in `--tkn_doc_id_column`) and `token_count`. 
The tokenizer will skip empty rows in the input table or rows leading to failure and track their counting in the `metadata`.


The parameter `--tkn_chunk_size` is used when each document is tokenized by chunks (of characters). Its defaut value is `0` 
which tokenize each document as a whole no matter how long it is. Chunks are round up by words, that means, the last word in a chunk
will not be split into half. Though this works for most languages having spaces among words, there is a very preliminary version
for languages having no space among words such as `ja` and it is highly recommended to customize such script for each particular language (specified via `--tkn_text_lang`)


