import gzip
import logging
import os
from pathlib import Path
from typing import Any

import boto3


KB = 1024
MB = 1024 * KB
GB = 1024 * MB


class Local:
    def __init__(
        self,
        folder_location: str,
    ) -> None:
        self.folder_location = folder_location

    def human_friendly_print(self, value: int) -> str:
        """
        Print the size of a file in a human-friendly format
        :param value: the size of the file, expressed in bytes
        :return: the file size expressed as a flost with 3 significant digits in GB, MB, KB or bytes, depending on size
        """
        if value >= GB:
            return f"{(value / GB):.3f} GB"
        elif value >= MB:
            return f"{(value / MB):.3f} MB"
        elif value >= KB:
            return f"{(value / KB):.3f} KB"
        else:
            return f"{value} Bytes"

    def calculate_size_statistics(self, data: list[dict]) -> dict:
        """
        Calculates max, min, and avg size from a list of list of dictionaries with string file names and integer file sizes.
        :param data (list[dict]): A list of dictionaries where each dictionary has a string key and an integer "Size" value.
        :return dict: A dictionary containing the maximum size, minimum size, average size and total number of files.
        """

        # Extract sizes and handle potential errors
        sizes = [d.get("size") for d in data if isinstance(d.get("size"), int)]

        if not sizes:
            raise ValueError("No valid size values found in the data.")

        # Calculate statistics using list comprehensions and store in dictionary
        file_size_stats = {
            "max_file_size": self.human_friendly_print(max(sizes)),
            "min_file_size": self.human_friendly_print(min(sizes)),
            "avg_file_size": self.human_friendly_print(sum(sizes) / len(sizes)),
            "total_num_files": len(sizes),
        }
        return file_size_stats

    # list the files in a local directory
    def list_files(self, url: str, file_ext: str = None, sort_by: str = None) -> tuple[list[dict], dict]:
        """
        Gets all files within a local file system directory, optionally filtered by extensions.
        :param url: the S3 path to search.
        :param file_ext: A list of file extensions (with the dot) to filter by. Defaults to None (all files).
        :param sort_by: sort the keys by name or size (decreasing order, largest files first). Defaults to None (no sorting)
        :return: A tuple with a list of dictionaries with the key and size of each file and a dictionary with file size stats
        """

        """
        Get files with the given extension for a given folder and all sub folders
        :param path: starting path
        :param extensions: List of extensions, None - all
        :return: List of files
        """
        extensions = file_ext.split(",")
        files = []
        for f_path in sorted(Path(url).rglob("*")):
            # for every file
            if f_path.is_dir():
                continue
            s_path = str(f_path.absolute())
            logging.info(s_path)
            if extensions is not None:
                _, extension = os.path.splitext(s_path)
                if extension not in extensions:
                    continue
            # elif "rdd" in s_path and "part" in s_path:
            size = Path(s_path).stat().st_size
            files.append(
                {
                    "name": s_path,
                    "size": size,
                }
            )

        if sort_by == "name":
            files.sort(key=lambda obj: obj["name"])
        elif sort_by == "size":
            files.sort(key=lambda obj: obj.get("size", 0), reverse=True)
        elif sort_by == "-size":
            files.sort(key=lambda obj: obj.get("size", 0), reverse=False)

        try:
            file_size_stats = self.calculate_size_statistics(files)
        except ValueError as e:
            logging.error(f"Failed to calculate size stats: {e}")
            file_size_stats = {}

        return files, file_size_stats

    def read_file(self, path: str) -> bytes:
        """
        Gets the contents of a file as a byte array, decompressing gz files if needed.
        :param path: The path to the file.
        :return: The contents of the file as a byte array, or None if an error occurs.
        """
        try:
            if path.endswith(".gz"):
                with gzip.open(path, "rb") as f:
                    data = f.read()
            else:
                with open(path, "rb") as f:
                    data = f.read()
            return data
        except (FileNotFoundError, gzip.BadGzipFile) as e:
            logging.error(f"Error reading file {path}: {e}")
            raise e

    def write_file(self, path: str, data: bytes) -> dict[str, Any]:
        """
        Saves bytes to a file and returns a dictionary with file information.
        :param path: The full name of the file to save.
        :param data: The bytes data to save.
        :return: A dictionary with "name" and "size" keys if successful,
                 or None if saving fails.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            file_info = {"name": path, "size": os.path.getsize(path)}
            return file_info
        except Exception as e:
            logging.error(f"Error saving bytes to file {path}: {e}")
            return None


class S3(Local):
    def __init__(
        self,
        folder_location: str,
        aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY"),
        service_name: str = "s3",
        endpoint_url: str = "https://s3.us-east.cloud-object-storage.appdomain.cloud",
    ) -> None:
        Local.__init__(self, folder_location)
        self.client = boto3.client(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            service_name=service_name,
            endpoint_url=endpoint_url,
        )

    # separate the bucket and the prefix from an S3 URL
    def parse_url(self, url: str) -> tuple[str, str]:
        """
        Separate the bucket and the prefix from an S3 URL
        :param url: the S3 url (s3://bucket/prefix)
        :return: a tuple of strings: (bucket_name, prefix)
        """
        url_prefix = "s3://"
        _, url_body = url.split("://")
        tokens = url_body.split("/")
        bucket_name = tokens[0]
        prefix = ""
        if len(tokens) > 1:
            prefix = "/".join(tokens[1:])
        return (bucket_name, prefix)

    # list the files in an S3 path
    def list_files(self, url: str, file_ext: str = None, sort_by: str = None) -> tuple[list[dict], dict]:
        """
        Gets all keys within an S3 path, with content as byte arrays, optionally filtered by extensions.
        :param url: the S3 path to search.
        :param file_ext: A list of file extensions (with the dot) to filter by. Defaults to None (all files).
        :param sort_by: sort the keys by name or size (decreasing order, largest files first). Defaults to None (no sorting)
        :return: A tuple with a list of dictionaries with the key and size of each file and a dictionary with file size stats
        """
        bucket, prefix = self.parse_url(url)
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get("Contents", []):
                if file_ext is None or obj["Key"].endswith(file_ext):  # Example: .zst
                    files.append(
                        {
                            "name": f"s3a://{bucket}/{obj['Key']}",
                            "size": obj["Size"],
                        }
                    )
        if sort_by == "name":
            files.sort(key=lambda obj: obj["name"])
        elif sort_by == "size":
            files.sort(key=lambda obj: obj.get("size", 0), reverse=True)
        elif sort_by == "-size":
            files.sort(key=lambda obj: obj.get("size", 0), reverse=False)

        try:
            file_size_stats = self.calculate_size_statistics(files)
        except ValueError as e:
            logging.error(f"Failed to calculate size stats: {e}")
            file_size_stats = {}

        return files, file_size_stats

    # Reads file from s3, the method returns the file and respective extension
    def read_file(self, url: str) -> bytes:
        """
        Reads a file from an S3 path, as an array of bytes
        :param url: the S3 path to the file to read.
        :return: an array of bytes with the contents of the file
        """
        data = None
        bucket, prefix = self.parse_url(url)
        try:
            response = self.client.get_object(Bucket=bucket, Key=prefix)
            data = response["Body"].read()
        except Exception as e:
            raise RuntimeError(f"Error downloading file from S3: {e}")
        return data

    # upload a byte array to s3
    def write_file(self, url: str, data: bytes) -> dict:
        """
        Writes an array of bytes to a file under a specific S3 path
        :param url: the S3 path where the file should be written.
        :param data: an array of bytes (the contents of the file)
        :return: a dictionary with upload stats
        """
        bucket, prefix = self.parse_url(url)
        for n in range(3):
            try:
                return self.client.put_object(Bucket=bucket, Key=prefix, Body=data)
            except Exception as e:
                err_msg = f"Failed to upload file to URL {url}: {e}"
        raise RuntimeError(err_msg)
