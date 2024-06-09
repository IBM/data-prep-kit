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

import os
import tempfile
from argparse import ArgumentParser, Namespace

import pyarrow as pa
from data_processing.runtime.pure_python.runtime_configuration import (
    PythonTransformRuntimeConfiguration,
)
from data_processing.runtime.ray import RayTransformLauncher
from data_processing.runtime.ray.runtime_configuration import (
    RayTransformRuntimeConfiguration,
)
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import TransformUtils
from scancode import api


LICENSE_COPYRIGHT_REMOVE_PRAMS = "license_copyright_removal_prams"
COLUMN_KEY = "lcr_contents_column_name"
LICENSE_KEY = "lcr_license"
COPYRIGHT_KEY = "lcr_copyright"
DEFAULT_COLUMN = "contents"
DEFAULT_LICENSE = True
DEFAULT_COPYRIGHT = True

def file_generate(content):
    """
    Generate temporary file so that it can be passed to scancode-toolkit.
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_file:
            temp_file.write(content.encode('utf-8'))
            temp_file_path = temp_file.name
    except Exception as e:
        print(f"Failed to create file : {e}")
    return temp_file_path

def fetch_index(dict_data):
    """
    Extract License and copyright start and endline from dictonary
    """
    ignore_lines = []
    if dict_data.get('license_detections') != None:
        for licenses in dict_data.get('license_detections'):
                for match in licenses.get('matches'):   
                    start_line = match['start_line'] -1
                    end_line = match['end_line'] -1
                    ignore_lines.extend([i for i in range(start_line,end_line+1)])

    if dict_data.get('copyrights') != None:             
        for copyrights in dict_data.get('copyrights'):
            start_line = copyrights.get('start_line') - 1
            end_line =  copyrights.get('end_line') - 1
            ignore_lines.extend([i for i in range(start_line,end_line+1)])

    return ignore_lines

def check_empty_comment(code,ignore_lines):
    min_index = min(ignore_lines)
    max_index = max(ignore_lines)
    code_list = code.split('\n')
    if min_index != 0:
        min_index = min_index - 1
    
    if max_index <= len(code_list):
        max_index = max_index + 2

    for index in range(min_index,max_index):
        if all(not isinstance(x, (int, float, complex)) and not isinstance(x, str) or (isinstance(x, str) and not x.isalnum()) for x in code_list[index]):
            if index not in ignore_lines:
                ignore_lines.append(index)

    return ignore_lines

def remove_copyright(code):
    """
    Using scancode.api function to detecte and remove copyright.
    """
    file_path = file_generate(content=code)
    copyright_dict = api.get_copyrights(file_path)
    os.remove(file_path)
    ignore_lines = fetch_index(copyright_dict)
    if ignore_lines != []:
        modified_code = '\n'.join([
                line for i, line in enumerate(code.split('\n'), 0)
                if i not in ignore_lines
            ])
        return modified_code,ignore_lines!=[]
    else :
        return code,False

def remove_license(code):
    """
    Using scancode.api function to detecte and remove license.
    """
    file_path = file_generate(content=code)
    license_dict = api.get_licenses(file_path)
    os.remove(file_path)
    ignore_lines = fetch_index(license_dict)
    if ignore_lines != []:
        modified_code = '\n'.join([
                line for i, line in enumerate(code.split('\n'), 0)
                if i not in ignore_lines
            ])
        return modified_code,ignore_lines!=[]
    else :
        return code,False

def remove_license_copyright(code):

    file_path = file_generate(code)
    copyright_dict = api.get_copyrights(file_path)
    license_dict = api.get_licenses(file_path)
    os.remove(file_path)
    ignore_lines_license = fetch_index(license_dict)
    ignore_lines_copyright = fetch_index(copyright_dict)
    ignore_lines = ignore_lines_license + ignore_lines_copyright
    if ignore_lines != []:
        ignore_lines = check_empty_comment(code,ignore_lines)
        modified_code = '\n'.join([
                line for i, line in enumerate(code.split('\n'), 0)
                if i not in ignore_lines
            ])
        return modified_code,True
    else:
        return code,False



class LicenseCopyrightRemoveTransform(AbstractTableTransform):
    def __init__(self, config: dict):
        super().__init__(config)

        self.license_copyright_remove = config.get(LICENSE_COPYRIGHT_REMOVE_PRAMS)
        self.license_remove = self.license_copyright_remove.get('license',DEFAULT_LICENSE)
        self.copyright_remove = self.license_copyright_remove.get('copyright',DEFAULT_COPYRIGHT)

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:

        if not TransformUtils.validate_columns(table, [self.license_copyright_remove['contents_column_name']]):
            return [], {}

        contents = table.column(self.license_copyright_remove['contents_column_name']).to_pylist()
        updated_content = []
        remove_code_count = 0
        for content in contents:
            if self.license_remove and self.copyright_remove:
                new_content,detect = remove_license_copyright(content)
                if detect:
                    remove_code_count+=1
                updated_content.append(new_content)

            elif self.copyright_remove and not self.license_remove:
                new_content,detect = remove_copyright(content)
                if detect:
                    remove_code_count+=1
                updated_content.append(new_content)

            elif self.license_remove and not self.copyright_remove:
                new_content,detect = remove_license(content)
                if detect:
                    remove_code_count+=1
                updated_content.append(new_content)

            else : 
                return [table],{'Removed code count' : remove_code_count}

        updated_content = pa.array(updated_content)
        
        table = table.append_column('updated_contents',updated_content)

        return [table], {'Removed code count' : remove_code_count}


class LicenseCopyrightRemovalTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(name="license_copyright_remove", transform_class=LicenseCopyrightRemoveTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{COLUMN_KEY}",
            required=False,
            type=str,
            default=f"{DEFAULT_COLUMN}",
            help="Name of the column holds the data to process",
        )
        parser.add_argument(
            f"--{LICENSE_KEY}",
            required=False,
            type=bool,
            default=DEFAULT_LICENSE,
            help="Set False if license should not be removed",
        )
        parser.add_argument(
            f"--{COPYRIGHT_KEY}",
            required=False,
            type=bool,
            default=DEFAULT_COPYRIGHT,
            help="Set False if copyright should not be removed ",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        dargs = vars(args)

        self.params = {
            LICENSE_COPYRIGHT_REMOVE_PRAMS: {
                "contents_column_name": dargs.get("contents_column_name"),
                "license": dargs.get("license"),
                "copyright": dargs.get("copyright"),
            }
        }

        return True


class LicenseCopyrightRemovalRayTransformConfiguration(RayTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=LicenseCopyrightRemovalTransformConfiguration())


class LicenseCopyrightRemovalPythonTransformConfiguration(PythonTransformRuntimeConfiguration):
    def __init__(self):
        super().__init__(transform_config=LicenseCopyrightRemovalTransformConfiguration())


if __name__ == "__main__":
    launcher = RayTransformLauncher(LicenseCopyrightRemovalPythonTransformConfiguration())
    launcher.launch()
