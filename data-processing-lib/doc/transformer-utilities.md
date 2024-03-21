# Transform Utilities

A class [TransformUtils](../src/data_processing/utils/transform_utils.py) provides several methods that simplify 
transformer's implementation. Currently it includes the following methods:
* `deep_get_size` is the method to get the complete size of the Python object based on
  https://www.askpython.com/python/built-in-methods/variables-memory-size-in-python
  It supports Python structures: list, tuple and set
* `normalize_string` normalizes string, converting it to lower case and removing spaces, punctuation and CR
* `str_to_hash`convert string to 259 bit hash
* `str_to_int` getting an integer representing string by calculating string's hash
* `validate_columns` check whether required columns exist in the table
* `add_column` adds column to the table avoiding duplicates. If the column with the given name already exists it will 
be removed before it is added
* `validate_path` cleans up s3 path - Removes white spaces from the input/output paths
  removes schema prefix (s3://, http:// https://), if exists
  adds the "/" character at the end, if it doesn't exist
  removes URL encoding

It also contain two variables:
* `RANDOM_SEED` number that is used for methods that require seed
* `LOCAL_TO_DISK` rough local size to size on disk/S3

This class should be extended with additional methods, generally useful across multiple transformers and documentation 
should be added here 