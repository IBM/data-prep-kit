import os
import argparse
import pyarrow as pa
import pyarrow.csv as pv
from io import StringIO,BytesIO
from watsonxai import generateResponseWatsonx


def gen_combined_strings(list_str):
    combined_strings = []
    combined_string = "\nLibrary,Language,Category\n"
    for idx, entry in enumerate(list_str, start=1):
        entry_string = ",".join([f"{value}" for key, value in entry.items()])
        combined_string += f"{entry_string}\n"
        if idx % 30 == 0 or idx == len(list_str): 
            combined_strings.append(combined_string)
            combined_string = "Library,Language,Category\n"
    return combined_strings


def sanitize_table(table):
    sanitized_columns = []
    for column in table.columns:
        sanitized_data = column.to_pylist()  
        sanitized_data = [str(val).replace('"', '') for val in sanitized_data]
        sanitized_column = pa.array(sanitized_data)
        sanitized_columns.append(sanitized_column)
    sanitized_table = pa.table(sanitized_columns, names=table.column_names)
    return sanitized_table

parser = argparse.ArgumentParser(description='Generate IKB.')
parser.add_argument('--null_libs_file', type=str, help='Path to null libraries file.', default=os.getenv('NULL_LIBS_FILE', '../ikb/null_libs.csv'))
parser.add_argument('--cmap_file', type=str, help='Path to concept map file.', default=os.getenv('CMAP_FILE', '../concept_map/updated_concept_list.csv'))
parser.add_argument('--input_examples_file', type=str, help='Path to input examples file.', default=os.getenv('EXAMPLES_I_FILE', '../examples/examples-i.csv'))
parser.add_argument('--output_examples_file', type=str, help='Path to output examples file.', default=os.getenv('EXAMPLES_O_FILE', '../examples/examples-o.csv'))
parser.add_argument('--extracted_data_file', type=str, help='Path to file in which LLM output will be stored.', default=os.getenv('EXTRACTED_DATA_FILE', '../ikb/extracted_data.csv'))
parser.add_argument('--api_type', type=str, help='API Type', default=os.getenv('API_TYPE', 'WatsonxAI'))
parser.add_argument('--api_key', type=str, help='API key', default=os.getenv('API_KEY', ''))
parser.add_argument('--api_endpoint', type=str, help='API endpoint', default=os.getenv('API_ENDPOINT', 'https://us-south.ml.cloud.ibm.com'))
parser.add_argument('--project_id', type=str, help='Project ID', default=os.getenv('PROJECT_ID', ''))
parser.add_argument('--model_id', type=str, help='LLM model ID', default=os.getenv('MODEL_ID', 'meta-llama/llama-3-70b-instruct'))




args = parser.parse_args()
concepts_list = pv.read_csv(args.cmap_file).column('Category').to_pylist()
concepts = ', '.join(concepts_list) 

csv_buffer_i = BytesIO()
pv.write_csv(pv.read_csv(args.input_examples_file), csv_buffer_i)
input_examples = csv_buffer_i.getvalue()

csv_buffer_o = BytesIO()
pv.write_csv(pv.read_csv(args.output_examples_file), csv_buffer_o)
output_examples = csv_buffer_o.getvalue()

cols=['Library', 'Language']
table = pv.read_csv(args.null_libs_file, read_options=pv.ReadOptions(column_names=cols))
null_library_names = [{col: table[i][j].as_py() for i, col in enumerate(cols)} for j in range(len(table))]
combined_strings = gen_combined_strings(null_library_names) 
endtoken = "<end>"

prompt_name = "My-prompt"
prompt_template = '''You are responsible for classifying programming language packages based on their functionality into one of the following STRICT categories:
                    ''' + concepts + '''

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
                    ''' + str(input_examples) + "OUTPUT:\n" + str(output_examples).strip("\n")+"\n<end>"


for combined_string in combined_strings:
    input_template = prompt_template + f"\n\nINPUT: {combined_string} \nOUTPUT: "
    if args.api_type == 'WatsonxAI':
        response = generateResponseWatsonx(args.api_key, args.api_endpoint, args.model_id, args.project_id, input_template)
    data = response.split(endtoken)[0]  
    csv_file = BytesIO(data.strip().encode('utf-8')) 
    table = pv.read_csv(csv_file)
    table = sanitize_table(table)
    with open(args.extracted_data_file, mode='ab') as f:
        pv.write_csv(table, f, write_options=pv.WriteOptions(include_header=False))


        




