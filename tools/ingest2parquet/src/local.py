import os
import sys
from pathlib import Path

from data_processing.utils import ParamsUtils
from ingest2parquet import run


if __name__ == "__main__":
    input_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "test-data", "input")
    )
    output_folder = os.path.join(
        os.path.join(os.path.dirname(__file__), "..", "test-data"), "output"
    )
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    local_conf = {
        "input_folder": input_folder,
        "output_folder": output_folder,
    }
    params = {
        "local_config": ParamsUtils.convert_to_ast(local_conf),
        "detect_programming_lang": True,
        "snapshot": "github",
        "domain": "code",
    }

    sys.argv = ParamsUtils.dict_to_req(d=params)
    run()
