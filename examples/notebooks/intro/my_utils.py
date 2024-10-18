import os
import requests
from humanfriendly import format_size
import pandas as pd
import glob


## Reads parquet files in a folder into a pandas dataframe
def read_parquet_files_as_df (parquet_dir):
    parquet_files = glob.glob(f'{parquet_dir}/*.parquet')

    # read each parquet file into a DataFrame and store in a list
    dfs = [pd.read_parquet (f) for f in parquet_files]

    # Concatenate all DataFrames into a single DataFrame
    data_df = pd.concat(dfs, ignore_index=True)
    return data_df


def download_file(url, local_file, chunk_size=1024*1024):
    """
    Downloads a remote URL to a local file.

    Args:
        url (str): The remote URL.
        local_filename (str): The name of the local file to save the downloaded content.
        chunk_size (int): The size in bytes of each chunk. Defaults to 1024.

    Returns:
        None
        
    Example usage:
        download_file('http://example.com/file.txt', 'file.txt', chunk_size=1024*1024)  # Download in chunks of 1MB
    """
    # Check if the local file already exists
    if os.path.exists(local_file):
        file_size = format_size(os.path.getsize(local_file))
        print(f"Local file '{local_file}' ({file_size}) already exists. Skipping download.")
        return

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(local_file), exist_ok=True)

    # Stream the file download
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        print()
        file_size = format_size(os.path.getsize(local_file))
        print(f"{local_file} ({file_size}) downloaded successfully.")
## --- end: download_file ------

