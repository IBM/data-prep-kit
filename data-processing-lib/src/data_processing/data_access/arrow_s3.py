# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from data_processing.utils import get_logger


logger = get_logger(__name__)


class ArrowS3:
    """
    Class replacing direct access to S3/COS by Pyarrow's `fs.S3FileSystem`. It uses Boto3 to interact
    with S3/COS and pyarrow to convert between Arrow table and binary. Usage of Boto3 for S3/COS access
    proves to be significantly more reliable
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint: str = None,
        region: str = None,
        s3_retries: int = 10,
        s3_max_attempts=10,
    ) -> None:
        """
        Initialization
        :param access_key: s3 access key
        :param secret_key: s3 secret key
        :param endpoint: s3 endpoint
        :param region: s3 region
        :param s3_retries: number of S3 retries - default 10
        :param s3_max_attempts - boto s3 client internal retries - default 10
        """
        # Create boto S3 client
        self.s3_client = boto3.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
            region_name=region,
            config=Config(retries={"max_attempts": s3_max_attempts, "mode": "standard"}),
        )
        self.retries = s3_retries

    @staticmethod
    def _get_bucket_key(key: str) -> tuple[str, str]:
        """
        Splitting complete folder name into bucket and prefix used by boto3 client
        :param key: complete folder name
        :return: bucket name and prefix
        """
        prefixes = key.split("/")
        return prefixes[0], "/".join(prefixes[1:])

    # get list of the files (names and sizes) for a given prefix (including bucket name)
    def list_files(self, key: str) -> list[dict[str, Any]]:
        """
        List files in the folder (hierarchically going through all sub-folders)
        :param key: complete folder name
        :return: list of dictionaries, containing file names and length
        """
        bucket, prefix = self._get_bucket_key(key)
        # Use paginator here to get all the files rather then 1 page
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        for page in pages:
            # For every page
            for obj in page.get("Contents", []):
                # Get both file name and size
                files.append({"name": f"{bucket}/{obj['Key']}", "size": obj["Size"]})
        return files

    def list_folders(self, key: str) -> list[str]:
        """
        Get list of folders for folder
        :param key: complete folder
        :return: list of folders within a given folder
        """
        bucket, prefix = self._get_bucket_key(key)

        def _get_sub_folders(bck: str, p: str) -> list[str]:
            # use paginator
            paginator = self.s3_client.get_paginator("list_objects_v2")
            # use Delimiter to get folders just folders
            page_iterator = paginator.paginate(Bucket=bck, Prefix=p, Delimiter="/")
            sub_folders = []
            for page in page_iterator:
                # for every page
                for p in page.get("CommonPrefixes", []):
                    sub_folders.append(p["Prefix"])
                    # apply recursively
                    sub_folders.extend(_get_sub_folders(bck, p["Prefix"]))
            return sub_folders

        prefixes = _get_sub_folders(bck=bucket, p=prefix)
        # remove base prefix
        return [p.removeprefix(prefix) for p in prefixes]

    def read_file(self, key: str) -> bytes:
        """
        Read an s3 file by name
        :param key: complete path
        :return: byte array of file content or None if the file does not exist
        """
        bucket, prefix = self._get_bucket_key(key)
        for n in range(self.retries):
            try:
                obj = self.s3_client.get_object(Bucket=bucket, Key=prefix)
                return obj["Body"].read()
            except Exception as e:
                logger.error(f"failed to read file {key}, exception {e}, attempt {n}")
        logger.error(f"failed to read file {key} in {self.retries} attempts. Skipping it")
        return None

    def save_file(self, key: str, data: bytes) -> dict[str, Any]:
        """
        Save file to S3
        :param key: complete path
        :param data: byte array of the file content
        :return: dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure dict is None
        """
        bucket, prefix = self._get_bucket_key(key)
        for n in range(self.retries):
            try:
                return self.s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
            except Exception as e:
                logger.error(f"Failed to upload file to to key {key}, exception {e}")
        logger.error(f"Failed to upload file {key}, skipping it")
        return None

    def read_table(self, key: str, schema: pa.schema = None) -> pa.Table:
        """
        Get an arrow table from a file with a given name
        :param key: complete path
        :param schema: Schema used for reading table, default None
        :return: table or None if the read failed
        """
        # Read file as bytes
        data = self.read_file(key)
        if data is None:
            return None
        try:
            # convert bytes to arrow table.
            reader = pa.BufferReader(data)
            table = pq.read_table(reader, schema=schema)
            return table
        except Exception as e:
            logger.error(f"Failed to convert file {key} to arrow table, exception {e}. Skipping it")
            return None

    def save_table(self, key: str, table: pa.Table) -> tuple[int, dict[str, Any]]:
        """
        Save an arrow table to a file with a name
        :param key: complete path
        :param table: table to save
        :return: table size and a dictionary as
        defined https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        in the case of failure len is -1 and dict is None
        """
        try:
            # convert table to bytes
            writer = pa.BufferOutputStream()
            pq.write_table(table=table, where=writer)
            data = bytes(writer.getvalue())
        except Exception as e:
            logger.error(f"Failed to convert arrow table to byte array for key {key}, exception {e}. Skipping it")
            return -1, None
        # save bytes
        return len(data), self.save_file(key, data)

    def delete_file(self, key: str) -> None:
        """
        Delete file from S3
        :param key: complete path
        :return: None
        """
        bucket, prefix = self._get_bucket_key(key)
        for n in range(self.retries):
            try:
                self.s3_client.delete_object(Bucket=bucket, Key=prefix)
                return None
            except Exception as e:
                logger.error(f"failed to delete file {key}, exception {e}")
        return None

    def move_file(self, source: str, dest: str) -> None:
        """
        move file from source to destination
        :param source: complete source path
        :param dest: complete destination path
        :return: None
        """
        s_bucket, s_prefix = self._get_bucket_key(source)
        d_bucket, d_prefix = self._get_bucket_key(dest)
        # copy source to destination and then delete source
        copy_source = {"Bucket": s_bucket, "Key": s_prefix}
        for n in range(self.retries):
            try:
                self.s3_client.copy_object(CopySource=copy_source, Bucket=d_bucket, Key=d_prefix)
                self.delete_file(source)
                return None
            except Exception as e:
                logger.error(f"failed to copy file {source} to {dest}, exception {e}")
        return None
