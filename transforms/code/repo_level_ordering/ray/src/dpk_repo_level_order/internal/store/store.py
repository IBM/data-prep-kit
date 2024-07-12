import os

from pyarrow.fs import FileSelector, FileType, LocalFileSystem, S3FileSystem


class FSStore:
    """
    This class behaves like a keyed value list store similar to dict[str,List[str]].
    This uses filesystem as backend to store keys as folders and values as filenames.

    This enables multiple parallel processes/nodes to use this data structure.
    This class supports local filesystem as well as s3 filesystem.

    Limitations of filesystem constrain the size of keys and values in this store.
    """

    def __init__(self, backend_path, s3_params=None):
        self.backend_path = backend_path
        self.s3 = None
        if s3_params is not None:
            self.fs = S3FileSystem(
                access_key=s3_params["access_key"],
                secret_key=s3_params["secret_key"],
                endpoint_override=s3_params["endpoint"],
                request_timeout=20,
                connect_timeout=20,
            )
            self.s3 = True
        else:
            self.fs = LocalFileSystem()
        self.failed_put_requests = []  # important in case of s3.

    def _normalize_key(self, key):
        return key.replace("/", "%2F")

    def _denormalize_key(self, key):
        return key.replace("%2F", "/")

    def put(self, key, item):
        # normalize keys, since we are creating folders for keys
        key = self._normalize_key(key)
        item = os.path.basename(item)
        self._write_values(self.fs, self.backend_path, key, item)

    def get(self, key):
        # normalize keys, since we are creating folders for keys
        key = self._normalize_key(key)
        return self._list_values(self.fs, self.backend_path, key)

    def items(self):
        normalized_keys = self._list_keys(self.fs, self.backend_path)
        return list(map(self._denormalize_key, normalized_keys))

    def remove(self, key):
        return self._delete_values(self.fs, self.backend_path, key)

    def __repr__(self):
        return f"keys: {self.items()}"

    def _list_values(self, filesystem, backend_path, key):
        key_path = os.path.join(backend_path, key)
        key_path = FileSelector(key_path)
        item = list(
            map(
                lambda x: x.base_name,
                list(
                    filter(
                        lambda x: x.is_file,
                        filesystem.get_file_info(key_path),
                    )
                ),
            )
        )
        return item

    def _list_keys(self, filesystem, backend_path):
        key_path = os.path.join(backend_path)

        key_path = FileSelector(key_path)
        item = list(
            map(
                lambda x: x.base_name,
                list(
                    filter(
                        lambda x: not x.is_file,
                        filesystem.get_file_info(key_path),
                    )
                ),
            )
        )

        return item

    def _write_values(self, filesystem, backend_path, key, value):
        key_path = os.path.join(backend_path, key)
        if not self.s3:
            filesystem.create_dir(key_path)
        entry_path = os.path.join(key_path, value)
        with filesystem.open_output_stream(entry_path) as stream:
            try:
                stream.write(b"data")
            except OSError:
                print("Failed writing value for {key}.")
                # Add the failed  key, value to queue. self.failed_put_requests, to
                # attempt to write again, in the flush function
                self.failed_put_requests = self.failed_put_requests.append((key, value))
                raise

    def _delete_values(self, filesystem, backend_path, key):
        key_path = os.path.join(backend_path, key)
        filesystem.delete_dir(key_path)

    def flush(self):
        print("Retry writing failed requests.")
        for key, value in self.failed_put_requests:
            self.put(key, value)


if __name__ == "__main__":
    p = "./mystore"
    store = FSStore(p)

    s3_store = {
        "secret_key": os.environ["AWS_SECRET_KEY"],
        "access_key": os.environ["AWS_ACCESS_KEY_ID"],
        "endpoint": "https://s3.us-south.cloud-object-storage.appdomain.cloud",
    }
    bk = "dev-code-datasets/test_sd/share/store"
    store = FSStore(bk, s3_store)

    store.put("doeswork", "qawe")

    store.put("abc__slash__upa", "a.pq")
    for key in store.items():
        print(f"values for {key}")
        print(store.get(key))
    print(store)
