import pandas as pd # type: ignore
from io import StringIO
import os
import csv
from ibm_watsonx_ai.foundation_models.utils.enums import ModelTypes


INPUT_UAST = 'input'
OUTPUT_UAST = 'output'
IKB_FILE = 'ikb/ikb_model.csv'
NULL_LIBS_FILE = "null.csv"

API_KEY = "Cl19NQn7D7y5ERFHfpUYNl8kWKqOTHqkGociOEI4nbsd"
API_ENDPOINT = "https://us-south.ml.cloud.ibm.com"
MODEL_ID = "meta-llama/llama-3-70b-instruct"
PROMPT_NAME = "My-prompt"
PROJECT_ID = "ba1b3e6d-5e38-4c72-9c36-4a9470cea282"

NEW_CMAP_FILE = "concept_map/updated_concept_list.csv"
NEW_CMAP = open(NEW_CMAP_FILE, 'r').read()
CONCEPTS = pd.read_csv(NEW_CMAP_FILE)['Category']


EXAMPLES_FILE_I = "examples/examples-i.csv"
df = pd.read_csv(EXAMPLES_FILE_I)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
EXAMPLES_I = csv_buffer.getvalue()

EXAMPLES_FILE_O = "examples/examples-o.csv"
df = pd.read_csv(EXAMPLES_FILE_O)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
EXAMPLES_O = csv_buffer.getvalue()

PROMPT_TEMPLATE_1_FINAL = '''You are responsible for classifying programming language packages based on their functionality into one of the following STRICT categories:
                ''' + NEW_CMAP + '''

                Instructions:                

                1. Input: A CSV containing two columns:
                          a. Library – the name of the package
                          b. Language – the programming language of the package
                    Your task is to append a third column called Category where you will classify the package's primary function into one of the following categories.\n
                                
                2. Output: The updated CSV with the new Category column.
                
                3. Categorization Guidelines:
                    a. Classify each package based on its primary functionality.
                    b. Only use categories from the given list. Do not invent or modify categories.
                
                4. Output format: Provide the updated CSV data in the exact format as shown below:
                    a. Columns: Library, Language, Category
                    b. End the response with <end> to indicate completion.

                5. Only use categories from the given list. Do not invent or modify categories.

                6. Strictly do not provide any explanations or commentary or notes before and/or after the table.
                
                Examples:
                INPUT:
                ''' + str(EXAMPLES_I) + "OUTPUT:\n" + str(EXAMPLES_O).strip("\n")+"\n<end>"



def init_config():
    # Create required folders
    folder_list = [OUTPUT_UAST]
    for folder in folder_list:
        if not os.path.exists(folder):
            os.makedirs(folder)
    # Create csv file
    if not os.path.exists(NULL_LIBS_FILE):
        with open(NULL_LIBS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Library', 'Language']
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
    return

