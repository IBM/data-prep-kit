import os
import argparse
import csv
import pyarrow as pa
import pyarrow.csv as pv
from io import StringIO,BytesIO
from watsonxai import generateResponseWatsonx


def getStringFromCSV(file):
    table = pv.read_csv(file)
    csv_buffer = StringIO()
    column_names = table.column_names
    csv_buffer.write(','.join(column_names) + '\n')
    for row in range(table.num_rows):
        row_data = [str(table[column][row].as_py()) for column in column_names]
        csv_buffer.write(','.join(row_data) + '\n')
    return csv_buffer.getvalue()
    


def gen_combined_strings(file_data):
    file_data = file_data.splitlines() 
    headers = file_data[0]
    null_libraries = file_data[1:]
    combined_strings = []
    combined_string = ""
    for idx, entry in enumerate(null_libraries, start=1):
        if combined_string == "":  
            combined_string += f"{headers.strip()}\n"
        combined_string += f"{entry}\n"
        if idx % 30 == 0 or idx == len(null_libraries): 
            combined_strings.append(combined_string)
            combined_string = ""
    return combined_strings


if __name__ == "__main__":

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
    concepts = getStringFromCSV(args.cmap_file)
    input_examples = getStringFromCSV(args.input_examples_file)
    output_examples = getStringFromCSV(args.output_examples_file)

    null_libs_file_data = getStringFromCSV(args.null_libs_file)
    combined_strings = gen_combined_strings(null_libs_file_data) 

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
                            c. Do not include any double quotes in the output.

                        5. Only use categories from the given list. Do not invent or modify categories.

                        6. Strictly do not provide any explanations or commentary or notes before and/or after the table.
                        
                        Examples:
                        INPUT:
                        ''' + str(input_examples) + "OUTPUT:\n" + str(output_examples).strip("\n")+"\n<end>"

    headers = ["Library", "Language", "Category"]
    file_exists = os.path.exists(args.extracted_data_file)
    if not file_exists:
        with open(args.extracted_data_file, mode='w', newline='') as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_NONE, escapechar='\\')
            csv_writer.writerow(headers) 


    for combined_string in combined_strings:
        input_template = prompt_template + f"\n\nINPUT: {combined_string} \nOUTPUT: "
        if args.api_type == 'WatsonxAI':
            response = generateResponseWatsonx(args.api_key, args.api_endpoint, args.model_id, args.project_id, input_template)  
        data = response.split(endtoken)[0]  
        csv_file = BytesIO(data.strip().encode('utf-8'))
        csv_content = data.splitlines()
        not_first_row = 0
        with open(args.extracted_data_file, mode='a', newline='') as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_NONE, escapechar='\\')
            for line in csv_content:
                if not_first_row:
                    row = line.split(',')
                    csv_writer.writerow(row)
                not_first_row = 1
        


            




