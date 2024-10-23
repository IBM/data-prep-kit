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
import os


class Detect_Programming_Lang:
    def __init__(self, path="lang_extensions.json"):
        file_path = os.path.join(os.path.dirname(__file__), path)
        print("filepath", file_path)
        with open(file_path, "r") as file:
            json_data = file.read()
        self.lang_ext_mapper = json.loads(json_data)
        self.reversed_mapper = {ext: langs for langs, exts in self.lang_ext_mapper.items() for ext in exts}

    def get_lang_from_ext(self, ext):
        lang = "unknown"
        if ext:
            lang_detected = self.reversed_mapper.get(ext, None)
            if lang_detected:
                lang = lang_detected
        return lang
