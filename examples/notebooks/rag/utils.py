import os
import requests
from humanfriendly import format_size
import pandas as pd
import glob

rootdir = os.path.abspath(os.path.join(__file__, "../../../../"))

## Reads parquet files in a folder into a pandas dataframe 
def read_parquet_files_as_df (parquet_dir):
    parquet_files = glob.glob(f'{parquet_dir}/*.parquet')

    # Create an empty list to store the DataFrames
    dfs = []

    # Loop through each Parquet file and read it into a DataFrame
    for file in parquet_files:
        df = pd.read_parquet(file)
        # print (f"Read file: '{file}'.  number of rows = {df.shape[0]}")
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    data_df = pd.concat(dfs, ignore_index=True)
    return data_df


def inspect_parquet (parquet_dir, sample_size = 5, display_columns = None):
    
    data_df = read_parquet_files_into_df (parquet_dir)

    if display_columns is not None:
        data_df = data_df[display_columns]
    #print(data_df.head(sample_size))
    return data_df.head(sample_size)


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

