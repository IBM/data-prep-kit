import hashlib
import os
import sys
from typing import IO


def get_file_extension(file_path) -> str:
    """
    Get the file extension from the given file path.
    Parameters:
    - file_path (str): The path of the file.
    Returns:
    - str: The file extension including the dot ('.') if present, otherwise an empty string.
    """
    ext = os.path.splitext(file_path)[1]
    if len(ext) > 0:
        return ext
    else:
        return ""


def get_hash(data: str) -> str:
    """
    Get the MD5 hash of the provided data.
    Parameters:
    - data (str): The data to be hashed.
    Returns:
    - str: The MD5 hash of the provided data, or an empty string if data is empty.
    """
    return hashlib.md5(data.encode("utf-8")).hexdigest() if data else ""


def get_file_size(data) -> int:
    """
    Get the size of the provided data in bytes.
    Parameters:
    - data: The data whose size needs to be determined.
    Returns:
    - int: The size of the data in bytes, or 0 if data is empty.
    """
    return sys.getsizeof(data) if len(data) > 0 else 0


def decode_content(content_bytes:bytes, encoding:str="utf-8")->str:
    """
    Decode the given bytes content using the specified encoding.
    Parameters:
    - content_bytes (bytes): The bytes content to decode.
    - encoding (str): The encoding to use while decoding the content. Default is 'utf-8'.
    Returns:
    - str: The decoded content as a string if successful,
                   otherwise empty string if an error occurs during decoding.
    """
    try:
        content_string = content_bytes.decode(encoding)
        return content_string
    except Exception as e:
        print(f"Error -> {e}")
        return ""
