import sys

from data_processing.utils import DPFConfig, ParamsUtils
from raw_data_to_parquet import run


# create parameters
s3_cred = {
    "access_key": DPFConfig.S3_ACCESS_KEY,
    "secret_key": DPFConfig.S3_SECRET_KEY,
    "url": "https://s3.us-south.cloud-object-storage.appdomain.cloud",
}
s3_conf = {
    "input_folder": "code-datasets/test-saptha/raw_to_parquet_guf",
    "output_folder": "code-datasets/test-saptha/raw_to_parquet_guf_out",
}
params = {
    "s3_cred": ParamsUtils.convert_to_ast(s3_cred),
    "s3_config": ParamsUtils.convert_to_ast(s3_conf),

}
sys.argv = ParamsUtils.dict_to_req(d=params)
if __name__ == '__main__':
    run()