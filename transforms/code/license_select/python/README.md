# License Select 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

The License Select transform checks if the `license` of input data is in approved/denied list. It is implemented as per the set of [transform project conventions](../../README.md#transform-project-conventions) the following runtimes are available:
This filter scans the license column of an input dataset and appends the `license_status` column to the dataset.

The type of the license column can be either string or list of strings. For strings, the license name is checked against the list of approved licenses. For list of strings, each license name in the list is checked against the list of approved licenses, and all must be approved.

If the license is approved, the license_status column contains True; otherwise False. 

## Configuration and command line Options

The set of dictionary keys holding license_select configuration for values are as follows:

The transform can be configured with the following key/value pairs from the configuration dictionary.

```python
# Sample params dictionary passed to the transform

{ 
"license_select_params" : {
        "license_column_name": "license",
        "deny_licenses": False,
        "licenses": [ 'MIT', 'Apache'],
        "allow_no_license": False,
    }
}
```

**license_column_name** - The name of the column with licenses.

**deny_licenses** - A boolean value, True for denied licesnes, False for approved licenses.

**licenses** - A list of licenses used as approve/deny list.

**allow_no_license** - A boolean value, used to retain the values with no license in the column `license_column_name` 


## Running

### Launcher Command Line Options 

The following command line arguments are available in addition to 
the options provided by the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

  `--lc_license_column_name` - set the name of the column holds license to process

  `--lc_allow_no_license` - allow entries with no associated license (default: false)

  `--lc_licenses_file` - S3 or local path to allowed/denied licenses JSON file

  `--lc_deny_licenses` - allow all licences except those in licenses_file (default: false)

- The optional `lc_license_column_name` parameter is used to specify the column name in the input dataset that contains the license information. The default column name is license.

- The optional `lc_allow_no_license` option allows any records without a license to be accepted by the filter. If this option is not set, records without a license are rejected.

- The required `lc_licenses_file` options allows a list of licenses to be specified. An S3 or local file path should be supplied (including bucket name, for example: bucket-name/path/to/licenses.json) with the file contents being a JSON list of strings. For example:

  >[
    'Apache-2.0',
    'MIT'
   ]

- The optional `lc_deny_licenses` flag is used when `lc_licenses_file` specifies the licenses that will be rejected, with all other licenses being accepted. These parameters do not affect handling of records with no license information, which is dictated by the allow_no_license option.


### Running the samples

To run the samples, use the following make targets

`run-cli-sample`

`run-local-python-sample` 

These targets will activate the virtual environment and set up any configuration needed. Use the -n option of make to see the detail of what is done to run the sample.

For example,
```
make run-cli-sample

```
...
Then

ls output
To see results of the transform.

### Transforming data using the transform image

To use the transform image to transform your data, please refer to the 
[running images quickstart](../../../../doc/quick-start/run-transform-image.md),
substituting the name of this transform image and runtime as appropriate.
