# import neccesary packages
from genai.client import Client
from genai.credentials import Credentials
from tree_sitter import Parser, Language
import json
from tree_sitter_languages import get_language
import glob
import os
from time import sleep
import streamlit as st
from annotated_text import annotated_text
import re
from config_LLM_runner_app import API_ENDPOINT, API_KEY

# Flag to dictate if it is concept-level pruning
GET_CONCEPTS_ONLY = False
# Flag to dictate if it is text based input
TEXT_TEST_CONCEPT = False

# enter your BAM API key here, or alternatively use os.environ
# You can alternatively, switch this to any model API. You have to change the request simlultaneously
if 'client' not in st.session_state:
    credentials = Credentials(api_key= API_KEY, api_endpoint = API_ENDPOINT)
    st.session_state['client'] = Client(credentials=credentials)

# load the cached requirements. This JSON contains important information about Concept nodes and language mapping to binding name.
if 'cached_requirements' not in st.session_state:
    st.session_state['cached_requirements'] = json.load(open('cached_requirements.json', 'r'))

# Load the neccesary maps. You can change them in the cached_requirements JSON and it will change dynamically.
###
formal_language_example_map = st.session_state['cached_requirements']['formal_language_example_map']
formal_language_map = st.session_state['cached_requirements']['formal_language_map']
formal_concept_map = st.session_state['cached_requirements']['formal_concept_map']
formal_model_card_map = st.session_state['cached_requirements']['formal_model_card_map']
concept_to_node_map = st.session_state['cached_requirements']['concept_to_node_map']
###

# option to select the few-shot examples
example_languages = st.sidebar.multiselect("Select the known languages to give few shot examples", list(formal_language_example_map.keys()))

# option to choose the test language. If it is not present here, look at the 'Adding new language' section in the documentation.
test_language = st.sidebar.selectbox("Select the unknown language you want to test", list(set(formal_language_map.keys()) - set(example_languages)))

# option to select the input method. If it is not present locally, change it to text-input 
test_method = st.sidebar.selectbox("How do you want to test?", ["Local Files", "User Input"])

# set the flag for text-based input
if (test_method == "User Input"):
    TEXT_TEST_CONCEPT = True

# initialise the snippet
test_code_snippet = None

# get input
if TEXT_TEST_CONCEPT:
    test_code_snippet = st.sidebar.text_area("Enter code snippet of the language used", height= 200)

# choose the concept to give ti=o extract rules for
test_concept = st.sidebar.selectbox("Select the UAST concept you want to extract", list(formal_concept_map.keys()))

# get the current few_shot examples present within the data.
present_examples = os.listdir('./data/few_shot_outputs/')

# file numbers are important as there can be multiple relevant nodes.
test_file_num = 0

# option to choose the model. 
model = st.sidebar.selectbox("Select the model you want to run the query on", list(formal_model_card_map.keys()))

# choose the pruning method.
pruning_method = st.sidebar.selectbox("Select the pruning method to apply to the example ASTs", ["Concept-Level Pruning", "No Pruning", "Depth-Level Pruning"])

# set to infinity for No-pruning.
max_depth = float('inf')

# set flags and depth levels for different techniques. Giving the option to choose depth.
if (pruning_method == "Depth-Level Pruning"):
    max_depth = st.sidebar.slider('Select the pruning depth of the AST', min_value= 1, max_value= 5, value = 3)

elif (pruning_method == "Concept-Level Pruning"):
    GET_CONCEPTS_ONLY = True
    max_depth = st.sidebar.slider('Select the pruning depth of the test AST', min_value = 1, max_value = 5, value= 3)

# few-shot example languages
example_languages = [formal_language_map[lang] for lang in example_languages]

# test language.
test_language = formal_language_map[test_language]

# get the formal concept name 
test_concept = formal_concept_map[test_concept]

# get the full model name
model = formal_model_card_map[model]

# map to store number of present examples.
if 'number_of_examples' not in st.session_state:
    st.session_state['number_of_examples'] = dict()

# save in session state
st.session_state['Languages'] = example_languages

# if its to fetch from local storage, append the test to the example-languages.
if not TEXT_TEST_CONCEPT:
    st.session_state['Languages'] = example_languages + [test_language]


"""
Function to convert and AST node into a string with requiring only relevant data.
Requires the ID of the node, the node type, the code snippet and the parent id.
"""
def create_node(id, node, parent_id):
    req_string = f"< node_id = {id}, node_type = {node.type}, code_snippet = {repr(node.text.decode('utf8'))}, parent_id = {parent_id} >"
    return req_string

"""
Function to recursively assign ID and preprocess the AST in a concept-level pruning manner to get it into a parse-able format to pass to the LLM.
dfs_id() function allocates a unique ID on preorder traversal basis to the treenode.
_dfs() function recursively parses the tree to the relevant node, while storing the code snippet relevant to a unique ID node.
"""
def get_concept_tree(tree, language):
    ast_repr = []
    code_snippets = dict()
    id_dictionary = dict()

    def dfs_id(node):
        id_dictionary[node] = len(id_dictionary)
        for child in node.children:
            dfs_id(child)

    dfs_id(tree.root_node)
    
    def _dfs(node, parent):
        if (node.type in concept_to_node_map[language][test_concept]):
            ast_repr.append(create_node(id_dictionary[node], node, id_dictionary[parent]))
            code_snippets[id_dictionary[node]] = node.text.decode("utf8")
        for child in node.children:
            _dfs(child, node)

    for child in tree.root_node.children:
        _dfs(child, tree.root_node)
    
    return ast_repr, code_snippets


"""
Function to recursively assign ID and preprocess the AST in a K-level-depth pruning manner to get it into a parse-able format to pass to the LLM.
dfs_id() function allocates a unique ID on preorder traversal basis to the treenode.
_dfs() function recursively parses the tree to the relevant node, while storing the code snippet relevant to a unique ID node.
"""
def get_tree(tree, k):
    ast_repr = []
    code_snippets = dict()
    id_dictionary = dict()

    def dfs_id(node):
        id_dictionary[node] = len(id_dictionary)
        for child in node.children:
            dfs_id(child)

    dfs_id(tree.root_node)
    
    def _dfs(node, depth, parent):
        if (depth >= k):
            return
        ast_repr.append(create_node(id_dictionary[node], node, id_dictionary[parent]))
        code_snippets[id_dictionary[node]] = node.text.decode("utf8")
        for child in node.children:
            _dfs(child, depth + 1, node)

    # _dfs(tree.root_node, -1, tree.root_node)
    for child in tree.root_node.children:
        _dfs(child, 0, tree.root_node)
    
    return ast_repr, code_snippets

# initialise an AST parser.
parser = Parser()

# use bindings from tree_sitter_language library. 
if 'language_binding' not in st.session_state:
    st.session_state['language_binding'] = {
        "cpp" : get_language("cpp"),
        "py" : get_language('python'),
        "java" : get_language("java"),
        "go" : get_language("go"),
        "js" : get_language("javascript"),
        "ts" : get_language("typescript"),
        "perl" : get_language("perl"),
        "php" : get_language("php"),
        "ocaml" : get_language("ocaml")
    }
    # uising the normal tree-sitter bindings locally for the laguages present in the cached_requirements json.
    for binding in os.listdir('../../input/tree-sitter-bindings'):
        name = binding.split('-bindings', 1)[0]
        # print(name)
        if name in st.session_state['language_binding']:
            continue
        try:
            language_obj = Language('tree-sitter-bindings/' + binding, name)
        except Exception as e:
            print(e)
            print(name)
            exit()
        st.session_state['language_binding'][name] = language_obj

#initialize session states to contain all the outputs.
if 'all_few_shot_outputs' not in st.session_state:
    st.session_state['all_few_shot_outputs'] = dict()

if 'all_asts' not in st.session_state:
    st.session_state['all_asts'] = dict()

if 'all_code_snippets' not in st.session_state:
    st.session_state['all_code_snippets'] = dict()

if 'all_concept_code_json' not in st.session_state:
    st.session_state['all_concept_code_json'] = dict()


# get all the few_shot LLM output examples present locally
def get_all_few_shot(example_languages, test_concept, language):
    for language in example_languages:
        programs = os.listdir(f"./data/few_shot_outputs/uast_{test_concept}/{language}")
        names = [os.path.basename(file).split('.')[0] for file in programs]
        for i in range(len(programs)):
            if (language not in st.session_state['all_few_shot_outputs']):
                st.session_state['all_few_shot_outputs'][language] = dict()

            content = open(f"./data/few_shot_outputs/uast_{test_concept}/{language}/{programs[i]}", "r").read()
            st.session_state['all_few_shot_outputs'][language][names[i]] = content

""" get all the few_shot code examples present locally and their corresponding AST with given max depth. 
This function also calls the AST preprocessor to store it in a global dictionary to retrieve in one step.
"""
def get_all_asts_code(test_concept, max_depth = 0):
    for language in st.session_state['Languages']:
        parser.set_language(st.session_state['language_binding'][language])
        programs = os.listdir(f"./data/Concept_dataset/uast_{test_concept}/{language}")
        names = [os.path.basename(file).split('.')[0] for file in programs]
        st.session_state['number_of_examples'][language] = len(programs)
        for i in range(len(programs)):
            if (language not in st.session_state['all_asts']):
                st.session_state['all_asts'][language] = dict()
                st.session_state['all_code_snippets'][language] = dict()
                st.session_state['all_concept_code_json'][language] = dict()

            content = open(f"./data/Concept_dataset/uast_{test_concept}/{language}/{programs[i]}", "r").read()
            st.session_state['all_code_snippets'][language][names[i]] = content
            ast = parser.parse(bytes(content, "utf8"))
            all_ast, all_code = None, None
            if (GET_CONCEPTS_ONLY and (language != test_language)):
                all_ast, all_code = get_concept_tree(ast, language)
            else:
                all_ast, all_code = get_tree(ast, max_depth)  
            st.session_state['all_asts'][language][names[i]] = str(all_ast)
            st.session_state['all_concept_code_json'][language][names[i]] = all_code

""" get all the corresponding AST with given max depth of the given text-input. 
This function also calls the AST preprocessor to store it in a global dictionary to retrieve in one step.
"""
def get_text_test_example(language, test_code_snippet):
    parser.set_language(st.session_state['language_binding'][language])
    if (language not in st.session_state['all_asts']):
        st.session_state['all_asts'][language] = dict()
        st.session_state['all_code_snippets'][language] = dict()
        st.session_state['all_concept_code_json'][language] = dict()
    st.session_state['all_code_snippets'][language]['0'] = test_code_snippet
    ast = parser.parse(bytes(test_code_snippet, "utf8"))
    all_ast, all_code = get_tree(ast, max_depth)
    st.session_state['all_asts'][language]['0'] = str(all_ast)
    st.session_state['all_concept_code_json'][language]['0'] = all_code
                
# load the prompt for the concept
category_prompt_file = f"./data/prompts/{test_concept}.txt"
st.session_state['prompt'] = open(category_prompt_file, "r").read()

# preprocessor for using the AST and code to convert it into a string
def example_builder(lang, program_num):
    return f"<code_snippet>\n{st.session_state['all_code_snippets'][lang][str(program_num)]}\n\n<AST>\n{st.session_state['all_asts'][lang][str(program_num)]}" 

# get the fewshot examples in a pluggable form to the LLM.
def get_few_shot():
    few_shot_examples = []
    for lang in example_languages:
        for program_num in range(st.session_state['number_of_examples'][lang]):
            few_shot_examples.append(
                {
                    "input" : f"{example_builder(lang, program_num)}",
                    "output" : f"{st.session_state['all_few_shot_outputs'][lang][str(program_num)]}"
                }
            )
    return few_shot_examples

# call funtions to get all such examples, codes and ASTs.
get_all_asts_code(test_concept, max_depth)
get_all_few_shot(example_languages, test_concept, test_language)
st.markdown("### Enter prompt here")

# make a modifiable prompt
st.session_state['prompt'] = st.text_area("prompt", st.session_state['prompt'], height= 700, label_visibility="collapsed")

# if its text-based call the function to get the AST.
if TEXT_TEST_CONCEPT:
    get_text_test_example(test_language, test_code_snippet)
st.session_state['test_input'] = f"{example_builder(test_language, '0')}"

# display the few-shot examples JSON
st.write('Training examples:')
st.write(get_few_shot())

# display the test JSON
st.write("Test example:")
st.write([st.session_state['test_input']])

"""
function to extract rule from the response. 
This works because of LLM alignment to generate response in a format, with the help of few-shot examples. 
"""
def get_rule_py(output_text):
    content = output_text.split('```py', 1)[1].split('```', 1)[0].strip()
    return content

"""
function to extract node type from the response. 
This works because of LLM alignment to generate response in a format, with the help of few-shot examples. 
"""
def extract_node_type(output_text):
    content = output_text.split('see that the', 1)[1].split('nodes', 1)[0].strip()
    return content.strip('\'"')

"""
function to extract IDs of all the relevant nodes from the response.
Returns a list of relevant node IDs.
This works because of LLM alignment to generate response in a format, with the help of few-shot examples. 
"""
def extract_node_id(output_text):
    content = None
    try:
        content = output_text.split('with ids = [', 1)[1].split(']', 1)[0].strip()
    except:
        try:
            content = output_text.split('with id = ', 1)[1].split(',', 1)[0].strip()
        except:
            st.write("cant be extracted")
    
    if (',') not in content:
        return [int(content)]
    
    id_strings = content.split(',')
    return [int(id.strip()) for id in id_strings]

"""
function to save the output generated by the LLM.
"""
def save_rule(language, node_type, rule, prompt, output, concept, ruleset_path, example_path, example_languages, test_code, max_depth):
    ruleset_files = os.listdir(ruleset_path)
    print(ruleset_files)

    # if the file is already present then just add a new mapping from the relevant node type to its corresponding rule.
    if (f'UAST_rules_{language}.json' in ruleset_files):
        rule_dict = json.load(open(f'{ruleset_path}/UAST_rules_{language}.json', 'r'))
        rule_dict[node_type] = {
                "uast_node_type": f"uast_{concept}",
                "extractor": rule
            }
    # if it is not, then make a new dictionary with the same.
    else:
        rule_dict = {
            node_type : {
                "uast_node_type": f"uast_{concept}",
                "extractor": rule
            }
        }

    print("saving rule for",language)
    try: 
        try:
            # try to save the rule dictionary
            json.dump(rule_dict, open(f'{ruleset_path}/UAST_rules_{language}.json', 'w'), indent = 4)
            print("json saved")
        except Exception as e:
            print("could not save rule JSON :", end = " ")
            print(e)

        # make the directory to save the output.
        os.makedirs(example_path + '/' + concept + '/' + language, exist_ok= True)
        files_present = os.listdir(f"{example_path}/{concept}/{language}")
        
        # loop to check already present files. This is because of multiple relevant nodes.
        counter = 0
        while(f"{counter}.txt" in files_present):
            counter += 1

        # saving the LLM output, input code, few-shot languages and the prompt.
        with open(f"{example_path}/{concept}/{language}/{counter}.txt", "w") as f:
            f.write(output)
            
        with open(f"{example_path}/{concept}/{language}/prompt_{counter}.txt", "w") as f:
            f.write(prompt)
            
        with open(f"{example_path}/{concept}/{language}/example_languages_{counter}.txt", "w") as f:
            f.write(str(example_languages) + '\n' + 'max_depth = '+ str(max_depth))
            
        with open(f"{example_path}/{concept}/{language}/test_code_{counter}.txt", "w") as f:
            f.write(test_code)
        
        os.makedirs(f"./data/few_shot_outputs/uast_{concept}/{language}", exist_ok= True)
        os.makedirs(f"./data/Concept_dataset/uast_{concept}/{language}", exist_ok= True)

        # save the output as another few-shot example.
        with open(f"./data/few_shot_outputs/uast_{concept}/{language}/{counter}.txt", "w") as f:
            f.write(output)
        
        with open(f"./data/Concept_dataset/uast_{concept}/{language}/{counter}.txt", "w") as f:
            f.write(test_code)
        
        # if everything is successful, display balloons on the screen!.
        st.balloons()
        print("Voila! prompt worked before i did 8410510369114 attempts! ")
    except Exception as e:
        print("COULD NOT SAVE FOR", language, "because :", e)

    # add concept nodes in the cached_requirements and save it.
    if (concept in st.session_state['cached_requirements']['concept_to_node_map'][language]) :
        if (node_type not in st.session_state['cached_requirements']['concept_to_node_map'][language][concept]):
            st.session_state['cached_requirements']['concept_to_node_map'][language][concept].append(node_type)
    else :
        st.session_state['cached_requirements']['concept_to_node_map'][language][concept] = [node_type]
    

    concept_to_node_map = st.session_state['cached_requirements']['concept_to_node_map']
    json.dump(st.session_state['cached_requirements'], open("cached_requirements.json", "w"), indent= 4)

# remove new-line comments frmo the code that the LLM generates. This is done to reduce memory consumption, as the output is saved already for documentation purposes.
def remove_comments(text):
    return re.sub(r"^(#.*?$)\n", "", text, flags = re.MULTILINE)

# change the extracted keyword to self.extracted keyword to make it work for the parser.
def process_rule(text):
    return remove_comments(text).replace("extracted", "self.extracted")

# function to enable stream generation through yielding tokens.
response = None
def stream_data():
    for token in response:
        yield token.results[0].generated_text

# if the submit button is clicked, perform the subsequent operations:
if st.sidebar.button('Submit'):

    # Invoke the query to the LLM after collecting the pluggable codes and ASTs.
    with st.spinner('Language model is working ...'):
        response = st.session_state['client'].text.generation.create_stream(
                model_id= model,
                parameters = {
                    "decoding_method": "greedy",
                    "min_new_tokens": 1,
                    "max_new_tokens": 1024
                },
                moderations = dict(),
                prompt_id = "prompt_builder",
                data = {
                    "input": st.session_state['test_input'],
                    "instruction": st.session_state['prompt'],
                    "input_prefix": "Input:",
                    "output_prefix": "Output:",
                    "examples": get_few_shot()
                }
            )
        st.markdown('### Response:')
        # stream output
        ans = st.write_stream(stream_data)

    st.write('----------------------------------------------')
    
    # extract the nodes and IDs.
    nodes = extract_node_id(ans)

    # extract the rule.
    rule = get_rule_py(ans)

    # get the relevant code snippets from the IDs it extracted.
    code_snippets = [st.session_state['all_concept_code_json'][test_language][str(test_file_num)][node] for node in nodes]
    extracted = None

    # run the code for each snippet.
    for i in range(len(code_snippets)):
        code_snippet = code_snippets[i]
        exec(rule)
        st.write(f'for Node with ID = {nodes[i]} and code')
        st.write(f'```{test_language}\n{code_snippet}')
        annotated_text('The extracted part is', (extracted,'', 'rgba(10,50,170,0.5)'))
        st.write('----------------------------------------------')
    
    # One-click acceptance of rule.
    st.sidebar.button("Accept the given rule?", on_click= save_rule, args= [test_language, extract_node_type(ans), process_rule(rule), st.session_state['prompt'], ans, test_concept, "./ruleset", "./data/final_UI_outputs", example_languages, st.session_state['all_code_snippets'][test_language]['0'], max_depth])