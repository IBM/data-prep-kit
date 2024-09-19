# Filtering
Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

Filtering cleans up data by:
 * Removing the rows that do not meet a specific set of criteria.
 * Dropping the columns that are no longer needed (e.g. annotation columns, used for filtering rows).

## Configuration and command line Options

The set of dictionary keys holding [FilterTransform](src/filter_transform_spark.py) 
configuration for values are as follows:

* _filter_criteria_list_ - specifies the list of row filter criteria (in SQL WHERE clause format). Each filter criterion is a string. The default value of this parameter is `[]` (an empty list, meaning that all the rows in the input table will be kept). 
* _filter_logical_operator_ - specifies the logical operator that joins filter criteria (`AND` or `OR`). The default value of this parameter is `AND`.
* _filter_columns_to_drop_ - the list with the names of the columns to drop after row filtering is complete. The default value of this parameter is `[]` (an empty list, meaning that all the columns in the input table will be kept)

## Example
Consider a table with eight text documents, where each row has additional info about that document (date acquired, source URL, etc.), and a set of quality signals for that document.  

```
|----------|----------|----------|----------|----------|----------|---------|----------|----------|----------|---------|
│ document | title    | contents | date_acq | extra    | cluster  | ft_lang | ft_score | docq_tot | docq_mea | docq_pe │
│ ---      | ---      | ---      | uired    | ---      | ---      | ---     | ---      | al_words | n_word_l | rplex_s │
│ str      | str      | str      | ---      | struct[5 | i64      | str     | f64      | ---      | en       | core    │
│          |          |          | datetime | ]        |          |         |          | i64      | ---      | ---     │
│          |          |          | [ns]     |          |          |         |          |          | f64      | f64     │
|----------|----------|----------|----------|----------|----------|---------|----------|----------|----------|---------|
│ CC-MAIN- | https:// | BACKGROU | 2023-07- | {"applic | -1       | en      | 1.0      | 77       | 5.662338 | 226.5   │
│ 20190221 | www.sema | ND       | 05       | ation/ht |          |         |          |          |          |         │
│ 132217-2 | nticscho | The      | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01902211 | lar.org/ | Rhinitis |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | Control  |          | …        |          |         |          |          |          |         │
│          |          | …        |          |          |          |         |          |          |          |         │
│ CC-MAIN- | https:// | Travel + | 2023-07- | {"applic | -1       | en      | 1.0      | 321      | 5.05919  | 245.0   │
│ 20200528 | www.torn | Leisure: | 27       | ation/ht |          |         |          |          |          |         │
│ 232803-2 | osnews.g | The 5    | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 02005290 | r/en/tou | best     |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | res…     |          | …        |          |         |          |          |          |         │
│ CC-MAIN- | https:// | Stourbri | 2023-07- | {"applic | -1       | en      | 1.0      | 646      | 5.27709  | 230.3   │
│ 20190617 | www.stou | dge      | 04       | ation/ht |          |         |          |          |          |         │
│ 103006-2 | rbridgen | College  | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01906171 | ews.co.u | to close |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | BMe…     |          | …        |          |         |          |          |          |         │
│ CC-MAIN- | https:// | Our      | 2023-07- | {"applic | -1       | en      | 1.0      | 242      | 5.557851 | 407.2   │
│ 20180318 | www.univ | Guidance | 19       | ation/ht |          |         |          |          |          |         │
│ 184945-2 | ariety.c | Philosop | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01803182 | om/app/s | hy       |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | High     |          | …        |          |         |          |          |          |         │
│          |          | sch…     |          |          |          |         |          |          |          |         │
│ CC-MAIN- | http://h | Hukun    | 2023-07- | {"applic | -1       | en      | 1.0      | 169      | 4.840237 | 240.5   │
│ 20180120 | urur.com | Hurur    | 18       | ation/ht |          |         |          |          |          |         │
│ 083038-2 | /hukun-h | running  | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01801201 | urur-run | for Ward |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | 1 c…     |          | …        |          |         |          |          |          |         │
│ CC-MAIN- | https:// | Life's   | 2023-07- | {"applic | -1       | en      | 1.0      | 61       | 4.786885 | 244.0   │
│ 20180522 | www.chap | Reverie  | 18       | ation/ht |          |         |          |          |          |         │
│ 131652-2 | ters.ind | Kobo     | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01805221 | igo.ca/e | ebook |  |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | Sept…    |          | …        |          |         |          |          |          |         │
│ CC-MAIN- | http://w | Kamis,   | 2023-07- | {"applic | 18008253 | en      | 1.0      | 509      | 4.738703 | 224.6   │
│ 20181120 | ww.onedo | 10 Maret | 05       | ation/ht | 113      |         |          |          |          |         │
│ 130743-2 | llarfoll | 2016     | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01811201 | owers.co | Buy      |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | Twitter… |          | …        |          |         |          |          |          |         │
│ CC-MAIN- | http://w | Rory     | 2023-07- | {"applic | -1       | en      | 1.0      | 223      | 4.829596 | 167.5   │
│ 20171213 | ww.iron- | Fallon   | 09       | ation/ht |          |         |          |          |          |         │
│ 104259-2 | bru.co.u | joins    | 05:00:00 | tp; msgt |          |         |          |          |          |         │
│ 01712131 | k/rory-f | Bristol  |          | ype=resp |          |         |          |          |          |         │
│ …        | …        | Rovers…  |          | …        |          |         |          |          |          |         │
|----------|----------|----------|----------|----------|----------|---------|----------|----------|----------|---------|
```
#### Example 1 - two numerical filtering criteria joined by AND
To filter this table and only keep the documents that have between 100 and 500 words **and** a perplexity score less than 230, and furthermore, drop the `extra` and `cluster` columns, invoke filtering with the following parameters:
```
filter_criteria_list = ["docq_total_words > 100 AND docq_total_words < 500", "docq_perplex_score < 230"]
filter_logical_operator = "AND"
filter_columns_to_drop = ["extra", "cluster"]
```
This filter operation applied on the table above will return the following result:
```
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|
│ document    | title       | contents    | date_acquir | ft_lang | ft_score | docq_total_ | docq_mean_w | docq_perple │
│ ---         | ---         | ---         | ed          | ---     | ---      | words       | ord_len     | x_score     │
│ str         | str         | str         | ---         | str     | f64      | ---         | ---         | ---         │
│             |             |             | datetime[ns |         |          | i64         | f64         | f64         │
│             |             |             | ]           |         |          |             |             |             │
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|
│ CC-MAIN-201 | http://www. | Rory Fallon | 2023-07-09  | en      | 1.0      | 223         | 4.829596    | 167.5       │
│ 71213104259 | iron-bru.co | joins       | 05:00:00    |         |          |             |             |             │
│ -201712131… | .uk/rory-f… | Bristol     |             |         |          |             |             |             │
│             |             | Rovers…     |             |         |          |             |             |             │
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|

```

#### Example 2 - two numerical filtering criteria joined by OR
To filter this table and only keep the documents that have between 100 and 500 words **or** a perplexity score less than 230, and furthermore, drop the `extra` and `cluster` columns, invoke filtering with the following parameters:
```
filter_criteria_list = ["docq_total_words > 100 AND docq_total_words < 500", "docq_perplex_score < 230"]
filter_logical_operator = "OR"
filter_columns_to_drop = ["extra", "cluster"]
```
This filter operation applied on the table above will return the following result:
```
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|
│ document    | title       | contents    | date_acquir | ft_lang | ft_score | docq_total_ | docq_mean_w | docq_perple |
│ ---         | ---         | ---         | ed          | ---     | ---      | words       | ord_len     | x_score     │
│ str         | str         | str         | ---         | str     | f64      | ---         | ---         | ---         │
│             |             |             | datetime[ns |         |          | i64         | f64         | f64         │
│             |             |             | ]           |         |          |             |             |             │
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|
│ CC-MAIN-201 | https://www | BACKGROUND  | 2023-07-05  | en      | 1.0      | 77          | 5.662338    | 226.5       │
│ 90221132217 | .semanticsc | The         | 05:00:00    |         |          |             |             |             │
│ -201902211… | holar.org/… | Rhinitis    |             |         |          |             |             |             │
│             |             | Control …   |             |         |          |             |             |             │
│ CC-MAIN-201 | http://www. | Kamis, 10   | 2023-07-05  | en      | 1.0      | 509         | 4.738703    | 224.6       │
│ 81120130743 | onedollarfo | Maret 2016  | 05:00:00    |         |          |             |             |             │
│ -201811201… | llowers.co… | Buy         |             |         |          |             |             |             │
│             |             | Twitter…    |             |         |          |             |             |             │
│ CC-MAIN-201 | http://www. | Rory Fallon | 2023-07-09  | en      | 1.0      | 223         | 4.829596    | 167.5       │
│ 71213104259 | iron-bru.co | joins       | 05:00:00    |         |          |             |             |             │
│ -201712131… | .uk/rory-f… | Bristol     |             |         |          |             |             |             │
│             |             | Rovers…     |             |         |          |             |             |             │
│ CC-MAIN-202 | https://www | Travel +    | 2023-07-27  | en      | 1.0      | 321         | 5.05919     | 245.0       │
│ 00528232803 | .tornosnews | Leisure:    | 05:00:00    |         |          |             |             |             │
│ -202005290… | .gr/en/tou… | The 5 best  |             |         |          |             |             |             │
│             |             | res…        |             |         |          |             |             |             │
│ CC-MAIN-201 | https://www | Our         | 2023-07-19  | en      | 1.0      | 242         | 5.557851    | 407.2       │
│ 80318184945 | .univariety | Guidance    | 05:00:00    |         |          |             |             |             │
│ -201803182… | .com/app/s… | Philosophy  |             |         |          |             |             |             │
│             |             | High sch…   |             |         |          |             |             |             │
│ CC-MAIN-201 | http://huru | Hukun Hurur | 2023-07-18  | en      | 1.0      | 169         | 4.840237    | 240.5       │
│ 80120083038 | r.com/hukun | running for | 05:00:00    |         |          |             |             |             │
│ -201801201… | -hurur-run… | Ward 1 c…   |             |         |          |             |             |             │
|-------------|-------------|-------------|-------------|---------|----------|-------------|-------------|-------------|
```

#### Example 3 - two filtering criteria based on non-numerical (datetime and string) types

To filter this table and only keep the documents that were acquired between 2023-07-04 and 2023-07-08 and were downloaded using the `HTTPS` protocol, without dropping any columns, invoke filtering with the following parameters:
```
filter_criteria_list = ["date_acquired BETWEEN '2023-07-04' AND '2023-07-08'", "title LIKE 'https://%'"]
filter_logical_operator = "AND"
filter_columns_to_drop = []
```

This filter operation applied on the table above will return the following result:
```
|----------|----------|----------|----------|----------|----------|---------|----------|----------|----------|---------|
│ document | title    | contents | date_acq | extra    | cluster  | ft_lang | ft_score | docq_tot | docq_mea | docq_pe │
│ ---      | ---      | ---      | uired    | ---      | ---      | ---     | ---      | al_words | n_word_l | rplex_s │
│ str      | str      | str      | ---      | struct[5 | i64      | str     | f64      | ---      | en       | core    │
│          |          |          | datetime | ]        |          |         |          | i64      | ---      | ---     │
│          |          |          | [ns]     |          |          |         |          |          | f64      | f64     │
|----------|----------|----------|----------|----------|----------|---------|----------|----------|----------|---------|
│ CC-MAIN- | https:// | BACKGROU | 2023-07- | {"applic | -1      | en      | 1.0      | 77       | 5.662338 | 226.5    │
│ 20190221 | www.sema | ND       | 05       | ation/ht |         |         |          |          |          |          │
│ 132217-2 | nticscho | The      | 05:00:00 | tp; msgt |         |         |          |          |          |          │
│ 01902211 | lar.org/ | Rhinitis |          | ype=resp |         |         |          |          |          |          │
│ …        | …        | Control  |          | …        |         |         |          |          |          |          │
│          |          | …        |          |          |         |         |          |          |          |          │
│ CC-MAIN- | https:// | Stourbri | 2023-07- | {"applic | -1      | en      | 1.0      | 646      | 5.27709  | 230.3    │
│ 20190617 | www.stou | dge      | 04       | ation/ht |         |         |          |          |          |          │
│ 103006-2 | rbridgen | College  | 05:00:00 | tp; msgt |         |         |          |          |          |          │
│ 01906171 | ews.co.u | to close |          | ype=resp |         |         |          |          |          |          │
│ …        | …        | BMe…     |          | …        |         |         |          |          |          |          │
|----------|----------|----------|----------|----------|---------|---------|----------|----------|----------|----------|
```


## Running
You can run the Spark filter transform [filter_local.py](src/filter_local_spark.py) to filter the `test1.parquet` file in [test input data](test-data/input) to an `output` directory.  The directory will contain one or several filtered parquet files and the `metadata.json` file.

#### Running as Spark-based application
```
(venv) cma:spark$ python src/filter_local.py 
18:57:46 INFO - data factory data_ is using local data access: input_folder - /home/cma/de/data-prep-kit/transforms/universal/filter/spark/test-data/input output_folder - /home/cma/de/data-prep-kit/transforms/universal/filter/spark/output at "/home/cma/de/data-prep-kit/data-processing-lib/ray/src/data_processing/data_access/data_access_factory.py:185"
18:57:46 INFO - data factory data_ max_files -1, n_sample -1 at "/home/cma/de/data-prep-kit/data-processing-lib/ray/src/data_processing/data_access/data_access_factory.py:201"
18:57:46 INFO - data factory data_ Not using data sets, checkpointing False, max files -1, random samples -1, files to use ['.parquet'] at "/home/cma/de/data-prep-kit/data-processing-lib/ray/src/data_processing/data_access/data_access_factory.py:214"
18:57:46 INFO - pipeline id pipeline_id at "/home/cma/de/data-prep-kit/data-processing-lib/ray/src/data_processing/runtime/execution_configuration.py:80"
18:57:46 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'} at "/home/cma/de/data-prep-kit/data-processing-lib/ray/src/data_processing/runtime/execution_configuration.py:83"
18:57:46 INFO - spark execution config : {'spark_local_config_filepath': '/home/cma/de/data-prep-kit/transforms/universal/filter/spark/config/spark_profile_local.yml', 'spark_kube_config_filepath': 'config/spark_profile_kube.yml'} at "/home/cma/de/data-prep-kit/data-processing-lib/spark/src/data_processing_spark/runtime/spark/spark_execution_config.py:42"
24/05/26 18:57:47 WARN Utils: Your hostname, li-7aed0a4c-2d51-11b2-a85c-dfad31db696b.ibm.com resolves to a loopback address: 127.0.0.1; using 192.168.1.223 instead (on interface wlp0s20f3)
24/05/26 18:57:47 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/05/26 18:57:48 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18:57:50 INFO - files = ['/home/cma/de/data-prep-kit/transforms/universal/filter/spark/test-data/input/test1.parquet'] at "/home/cma/de/data-prep-kit/data-processing-lib/spark/src/data_processing_spark/runtime/spark/spark_launcher.py:188"
```

Then 
```shell
ls output
```
To see results of the transform.

#### Passing parameters through command-line-interface

When running filtering on a local terminal, double quotes need to be escaped accordingly. For example, to find documents that are written in Java or Python programming languages, a SQL query using the `IN` keyword is needed in the `filter_criteria_list` argument. The example below shows how to properly pass this argument to the filter app:
```    
python filter_transform_spark.py --filter_criteria_list "[\"language IN ('Java', 'Python')\"]" ...
```
When filter runs from the command line, it needs to include the entire `filter_criteria_list` parameter within double quotes (`"`), so that the command line parser can determine where the parameter begins and ends. This, however, will conflict with the internal double quotes that are used to specify the conditions inside the list (`language IN ('Java', 'Python')`). To resolve this problem, the internal double quotes need to be escaped, as in the \"language IN ('Java', 'Python')\" notation.

### Filter Statistics
As shown in the output of the local run of filtering, the metadata contains several statistics:
* Global statistics:  
  * `total_docs_count`, `total_bytes_count`, `total_columns_count`: total number of documents (rows), bytes, and columns that were present in the input table, before filtering took place  
  * `docs_after_filter`, `bytes_after_filter`, `columns_after_filter`: total number of documents (rows), bytes, and columns that were present in the output table, after filtering took place  
* Per-criteria statistics: these statistics show the impact of each filtering criteria - number of documents and bytes that it filters out, when applied by itself. We ran the local filter with two filtering criteria, and these are the stats for each of them:  
  * `docs_filtered_by 'docq_total_words > 100 AND docq_total_words < 200'`, `bytes_filtered_by 'docq_total_words > 100 AND docq_total_words < 200'` - the number of documents and bytes filtered out by the `docq_total_words > 100 AND docq_total_words < 200` filtering condition  
  * `docs_filtered_by 'docq_perplex_score < 230'`, `bytes_filtered_by 'docq_perplex_score < 230'`  - the number of documents and bytes filtered out by the `docq_perplex_score < 230` filtering condition  


## Running

### Launched Command Line Options 
When running the transform with the Spark launcher (i.e. SparkTransformLauncher),
the following command line arguments are available in addition to 
the options provided by the [spark launcher](../../../../data-processing-lib/doc/spark-launcher-options.md).

```
  --filter_criteria_list FILTER_CRITERIA_LIST
                        list of filter criteria (in SQL WHERE clause format), for example: [
                          "docq_total_words > 100 AND docq_total_words < 200",
                          "docq_perplex_score < 230",
                          "date_acquired BETWEEN '2023-07-04' AND '2023-07-08'",
                          "title LIKE 'https://%'",
                          "document_id IN ('doc-id-1', 'doc-id-2', 'doc-id-3')"
                        ]
  --filter_columns_to_drop FILTER_COLUMNS_TO_DROP
                        list of columns to drop after filtering, for example: ["column1", "column2"]
  --filter_logical_operator {AND,OR}
                        logical operator (AND or OR) that joins filter criteria

```

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
