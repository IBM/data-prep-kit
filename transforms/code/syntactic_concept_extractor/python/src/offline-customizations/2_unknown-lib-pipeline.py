import argparse
from llm_interaction import *
from config import *


prompt = PROMPT_TEMPLATE_1_FINAL

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", type=str, default=NULL_LIBS_FILE, help="File path")

args = parser.parse_args()
file_data = read_csv(args.file)
combined_strings = gen_combined_strings(file_data) 
input_data = {} 


for combined_string in combined_strings:
    input_template = prompt + f"\n\nINPUT: {combined_string} \nOUTPUT: "
    response = model.generate_text(input_template)
    print(response)
    save_result(response,'ikb/extracted_data.csv',"<end>")


    




