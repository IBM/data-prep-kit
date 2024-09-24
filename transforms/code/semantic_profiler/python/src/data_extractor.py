import os
import csv
import json
import pandas as pd
import re
from UAST import *
from config import *


def uast_read(x):
    uast = UAST()
    if x != 'null':
        uast.load_from_json_string(x)
        return uast
    else:
        return None

    
def extract_from_uast(uast):
    if not uast:
        return ""
    for id, node in uast.nodes.items():
        node_type = node.node_type
        if node_type == "uast_root":
            extracted_data = node.metadata["code_to_comment_ratio"]
    return extracted_data



def data_extractor(INPUT_UAST):
    df = pd.read_parquet(INPUT_UAST)
    uasts = df['UAST'].apply(uast_read)
    df['code_to_comment_ratio'] = uasts.apply(extract_from_uast)
    return df['Library'], df['Language'], df['code_to_comment_ratio']

