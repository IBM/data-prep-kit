# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import json

def extract_library_names(json_filepath, output_filepath):
    try:
        # Open and read the JSON file
        with open(json_filepath, 'r') as file:
            data = json.load(file)
        
        # Collect all 'library_name' values
        library_names = [item['package'] for item in data if 'package' in item]
        
        # Write these names to a text file
        with open(output_filepath, 'w') as file:
            for name in library_names:
                file.write(name + '\n')
        
        print(f"Library names were successfully written to {output_filepath}")
    
    except FileNotFoundError:
        print("Error: The specified file does not exist.")
    except json.JSONDecodeError:
        print("Error: The file is not a valid JSON.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
json_filepath = 'java_standard_packages.json'
output_filepath = 'java_standard_packages.txt'
extract_library_names(json_filepath, output_filepath)