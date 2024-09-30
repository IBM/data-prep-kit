import re
from io import StringIO
import pandas as pd
from config import *
from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import APIClient, Credentials


credentials = Credentials(api_key=API_KEY, url=API_ENDPOINT)

parameters = {
    GenParams.DECODING_METHOD: "greedy",
    GenParams.MAX_NEW_TOKENS: 1000,
    GenParams.STOP_SEQUENCES: ["<end>"]
}

model = ModelInference(
    model_id=MODEL_ID, 
    params=parameters, 
    credentials=credentials,
    project_id=PROJECT_ID)



def init_concept_map(cm_file):
    with open(cm_file, 'r') as file:
        concept_map = file.read()
    return concept_map

def read_csv(csv_file, cols=['Library', 'Language']):
    df = pd.read_csv(csv_file, usecols=cols)
    data = df.to_dict(orient='records')
    return data

def gen_combined_strings(list_str):
    combined_strings = []
    combined_string = "\nLibrary,Language,Category\n"
    for idx, entry in enumerate(list_str, start=1):
        entry_string = ",".join([f"{value}" for key, value in entry.items()])
        combined_string += f"{entry_string}\n"
        if idx % 30 == 0 or idx == len(list_str):  # Ensure to include the last batch
            combined_strings.append(combined_string)
            combined_string = "Library,Language,Category\n"
    return combined_strings



# def generate_response(input_template):
#     result = model.generate_text(input_template)
#     return result



def save_result(data, filename, endtoken):
    data = data.split(endtoken)[0]  # Split the data at the end token and take the first part
    csv_file = StringIO(data.strip())  # Remove any leading/trailing whitespace
    df = pd.read_csv(csv_file)
    print(df.columns)
    df.to_csv(filename, mode='a', index=False, header=False)
    return

def read_examples(file):
    df = pd.read_csv(file)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    examples = csv_buffer.getvalue()
    return examples


# if __name__ == "__main__":
#      CONCEPT_MAP_FILE = "/Users/adrijadhar/Documents/GitHub/code-semantic-analysis/Testing/Prompt 1/examples/new_concept_map.txt"
#      NEW_CMAP_FILE = "/Users/adrijadhar/Documents/GitHub/code-semantic-analysis/Testing/Prompt 1/examples/new_concept_map.txt"
#      df = pd.read_csv(CONCEPT_MAP_FILE, usecols=["Functionality"])
#      df.to_csv(NEW_CMAP_FILE, index=False)