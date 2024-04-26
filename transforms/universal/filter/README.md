# Filtering
Please see the set of
[transform project conventions](../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

Filtering cleans up data by:
 * Removing the rows that do not meet a specific set of criteria.
 * Dropping the columns that are no longer needed (e.g. annotation columns, used for filtering rows).

## Configuration and command line Options

The set of dictionary keys holding [FilterTransform](src/filter_transform.py) 
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
You can run the [filter_local.py](src/filter_local.py) (python-only implementation) or [filter_local_ray.py](src/filter_local_ray.py) (ray-based  implementation) to transform the `test1.parquet` file in [test input data](test-data/input) to an `output` directory.  The directory will contain both the new annotated `test1.parquet` file and the `metadata.json` file.

#### Running as ray-based application
```
(venv) cma:src$ python filter_local_ray.py
12:48:01 INFO - Running locally
12:48:01 INFO - Using local configuration with: input_folder - /home/cma/de/data-prep-lab/transforms/universal/filtering/test-data/input output_folder - /home/cma/de/data-prep-lab/transforms/universal/filtering/output
12:48:01 INFO - Not using data sets, checkpointing False, max files -1
12:48:01 INFO - number of workers 5 worker options {'num_cpus': 0.8}
12:48:01 INFO - pipeline id pipeline_id; number workers 5
12:48:01 INFO - job details {'job category': 'preprocessing', 'job name': 'filter', 'job type': 'ray', 'job id': 'job_id'}
12:48:01 INFO - code location {'github': 'github', 'commit_hash': '12345', 'path': 'path'}
12:48:01 INFO - actor creation delay 0
2024-03-31 12:48:03,390	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
(orchestrate pid=308034) 12:48:04 INFO - orchestrator started at 2024-03-31 12:48:04
(orchestrate pid=308034) 12:48:04 INFO - Number of files is 1, source profile {'max_file_size': 0.15915775299072266, 'min_file_size': 0.15915775299072266, 'total_file_size': 0.15915775299072266}
(orchestrate pid=308034) 12:48:04 INFO - Cluster resources: {'cpus': 20, 'gpus': 0, 'memory': 31.60095291212201, 'object_store': 15.800476455129683}
(orchestrate pid=308034) 12:48:04 INFO - Number of workers - 5 with {'num_cpus': 0.8} each
(orchestrate pid=308034) 12:48:04 INFO - Completed 0 files in 5.312760670979818e-06 min. Waiting for completion
(orchestrate pid=308034) 12:48:06 INFO - Completed processing in 0.022701112429300944 min
12:48:16 INFO - Completed execution in 0.24697633584340414 min, execution result 0
```
#### Running as pure python application
<pre>
% make venv
% source venv/bin/activate
(venv) % cd src
(venv) % python filter_local_ray.py
input table has 100 rows

output table has 11 rows
output metadata : {'total_docs_count': 100, 'total_bytes_count': 478602, 'total_columns_count': 25, "docs_filtered_by 'docq_total_words > 100 AND docq_total_words < 200'": 78, "bytes_filtered_by 'docq_total_words > 100 AND docq_total_words < 200'": 429191, "docs_filtered_by 'docq_perplex_score < 230'": 53, "bytes_filtered_by 'docq_perplex_score < 230'": 275911, 'docs_after_filter': 11, 'bytes_after_filter': 24061, 'columns_after_filter': 23}
(venv) % deactivate
% ls ../output
metadata.json	test1.parquet
%
</pre>

#### Passing parameters through command-line-interface

When running filtering on a local terminal, double quotes need to be escaped accordingly. For example, to find documents that are written in Java or Python programming languages, a SQL query using the `IN` keyword is needed in the `filter_criteria_list` argument. The example below shows how to properly pass this argument to the filter app:
```    
python filter_transform.py --filter_criteria_list "[\"language IN ('Java', 'Python')\"]" ...
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


### Building the Docker Image
```shell
% make image 
...

````
In addition, there are some useful `make` targets (see conventions above)
or use `make help` to see a list of available targets.

### Launched Command Line Options 
When running the transform with the Ray launcher (i.e. TransformLauncher),
the following command line arguments are available in addition to 
[the options provided by the launcher](../../../data-processing-lib/doc/launcher-options.md).
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

### Executing S3 examples

To execute S3 examples, please refer to this [document](../../../data-processing-lib/doc/using_s3_transformers.md)
for setting up MinIO and mc prior to running the example

