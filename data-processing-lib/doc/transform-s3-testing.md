# Testing transforms with S3

For testing transforms with S3 we are using [Minio](https://min.io/), which can be installed on
[Linux](https://min.io/docs/minio/linux/index.html), [macOS](https://min.io/docs/minio/macos/index.html) and
[Windows](https://min.io/docs/minio/windows/index.html). Here we are assuming Mac usage, refer to documentation
above for other platforms.

## Installing Minio

The simplest way to install Minio on Mac is using Homebrew. Use the following command:

```shell
brew install minio/stable/minio
```

In addition to the Minio server install the latest stable MinIO cli using

```shell
brew install minio/stable/mc
```
Now you can start Minio server using the following command:

```shell
minio server start
```

When it starts you can connect to the server UI using the following address: `http://localhost:9000`
The default user name/password is `minioadmin|minioadmin`

## Populating Minio with testing data

Populating Minio server with test data can be done using `mc`. First configure mc to work with the local
Minio server:


```shell
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
```

This set an alias `local` to 'mc' connected to the local Minio server instance. Now we can use our
mc instance to populate server using a set of
[commands](https://min.io/docs/minio/linux/reference/minio-mc.html) provided by `mc`.

First test the connection to the newly added MinIO deployment using the `mc admin info` command:

```shell
mc admin info local
```

To copy the data to Minio, you first need to create a bucket:

```shell
mc mb local/test
```

Once the bucket is created, you can copy files (assuming you are in the transforms directory), using:

```shell
mc cp --recursive tools/ingest2parquet/test-data/input/ local/test/ingest2parquet/input
mc cp --recursive code/code_quality/test-data/input/ local/test/code_quality/input
mc cp --recursive code/proglang_select/test-data/input/ local/test/proglang_select/input
mc cp --recursive code/proglang_select/test-data/languages/ local/test/proglang_select/languages
mc cp --recursive code/malware/test-data/input/ local/test/malware/input

mc cp --recursive language/doc_quality/test-data/input/ local/test/doc_quality/input
mc cp --recursive language/language_id/test-data/input/ local/test/language_id/input

mc cp --recursive universal/blocklist/test-data/input/ local/test/blocklist/input
mc cp --recursive universal/blocklist/test-data/domains/ local/test/blocklist/domains
mc cp --recursive universal/doc_id/test-data/input/ local/test/doc_id/input
mc cp --recursive universal/ededup/test-data/input/ local/test/ededup/input
mc cp --recursive universal/fdedup/test-data/input/ local/test/fdedup/input
mc cp --recursive universal/filter/test-data/input/ local/test/filter/input
mc cp --recursive universal/noop/test-data/input/ local/test/noop/input
mc cp --recursive universal/resize/test-data/input/ local/test/resize/input
mc cp --recursive universal/tokenization/test-data/ds01/input/ local/test/tokenization/ds01/input
mc cp --recursive universal/tokenization/test-data/ds02/input/ local/test/tokenization/ds02/input
```

*Note*, that once the data is copied, Minio is storing it on the local file system, so you do not need to
copy it again after cluster restart

## Creating access and secret key for Minio access

The last thing is to add Minio access and secret keys for accessing it. The following command:

```shell
mc admin user svcacct add --access-key "localminioaccesskey" --secret-key "localminiosecretkey" local minioadmin
```

creates both access and secret key for usage by the applications
