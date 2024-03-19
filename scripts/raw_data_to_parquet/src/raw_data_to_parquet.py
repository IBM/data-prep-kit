import argparse
import os
import uuid
import zipfile
from typing import Any,List
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
import pyarrow as pa
import utils 
from data_processing.data_access import DataAccess, DataAccessFactory
import os
import io

def zip_to_table(data_access:DataAccess, file_path):
        try:
            data=[]
            zip_name=os.path.basename(file_path)
            compressed_zip_bytes= data_access.get_file(file_path)
            with zipfile.ZipFile(io.BytesIO(bytes(compressed_zip_bytes))) as opened_zip:
                for member in opened_zip.infolist():
                    if not member.is_dir():
                        with opened_zip.open(member) as file:
                            try:
                                content_bytes = file.read()
                                content_string = utils.decode_content(content_bytes)
                                if content_string and len(content_string)>0:
                                    data.append(
                                        {
                                            "file_path": member.filename,
                                            "document": zip_name,
                                            "contents": content_string,
                                            "document_id": str(uuid.uuid4()),
                                            "ext": utils.get_file_extension(file_path),
                                            "hash": utils.get_hash(content_string),
                                            "size": utils.get_file_size(content_string),
                                            "date_acquired": datetime.now().isoformat(),
                                        }
                                    )
                                else:
                                    raise Exception("No contents decoded")

                            except Exception as e:
                                print(f" skipping {member.filename} Error: {str(e)}")

                
                if len(data) > 0:
                    df = pd.DataFrame(data)
                    table = pa.Table.from_pandas(df)
                    output_file_name = data_access.get_output_location(file_path).replace(".zip", ".parquet"
        )
                    return save_as_parquet(data_access,output_file_name, table)

        except Exception as e:
            return (False, {"file": file_path, "error": str(e)})
    
def raw_to_parquet(data_access_factory:DataAccessFactory, file_path):
    data_access=data_access_factory.create_data_access()

    ext = utils.get_file_extension(file_path)
    if ext == ".zip":
        return zip_to_table(data_access,file_path)

    else:
        return (
            False,
            {
                "name": file_path,
                "error": "Got {ext} file, not supported",
            },
        )


def save_as_parquet(data_access:DataAccess, file_name: str, table: pa.table):
    try:

        upload_metadata = data_access.save_table(file_name, table)
        if upload_metadata:
            return (True, upload_metadata)
        else:
            raise Exception("Failed to upload")
    except Exception as e:
        return (False, {"file": file_name, "error": str(e)})


def generate_stats(metadata:List[dict])-> dict[str,Any]:
    success = 0
    sucess_details=[]
    failures=0
    failure_details = []
    for m in metadata:
        if m[0] == True:
            success += 1
            sucess_details.append(m[1])
        else :
            failures+=1
            failure_details.append(m[1])

    success_df = pd.DataFrame(sucess_details, columns=['bytes_in_memory', 'file_info'])


    success_df['file_size_disk'] = success_df['file_info'].apply(lambda x: x["size"])
    total_bytes_disk = success_df['file_size_disk'].sum()
    total_bytes_in_memory = success_df['bytes_in_memory'].sum()

    return {
        "total_files_given":len(metadata),
        "total_files_processed":success,
        "total_files_failed_to_processed":failures,
        "total_file_size_on_disk":int(total_bytes_disk),
        "total_bytes_in_memory":int(total_bytes_in_memory),
        "failure_details":failure_details,
    }


def run():

    parser = argparse.ArgumentParser(description="raw-data-to-parquet")

    data_access_factory = DataAccessFactory()
    data_access_factory.add_input_params(parser)

    args = parser.parse_args()
    data_access_factory.apply_input_params(args)

    data_access = data_access_factory.create_data_access()
    
    file_paths = data_access.get_folder_files(
        data_access.input_folder, ["zip"], False
    )

    print("file_paths",file_paths)

    if len(file_paths) != 0:
        print(f"Number of files is {len(file_paths)} ")
        metadata=[]
        with Pool() as p:
            results = p.starmap_async(raw_to_parquet, [( data_access_factory, file_path,) for file_path in file_paths.keys()])
            metadata=results.get()

        stats = generate_stats(metadata)
        print(data_access.save_job_metadata(stats))