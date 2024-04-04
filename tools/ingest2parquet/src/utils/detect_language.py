import json
import os


class Detect_Programming_Lang:
    def __init__(self, path="lang_extensions.json"):
        file_path = os.path.join(os.path.dirname(__file__), path)
        print("filepath", file_path)
        with open(file_path, "r") as file:
            json_data = file.read()
        self.lang_ext_mapper = json.loads(json_data)
        self.reversed_mapper = {
            ext: langs for langs, exts in self.lang_ext_mapper.items() for ext in exts
        }

    def get_lang_from_ext(self, ext):
        lang = "unknown"
        if ext:
            lang_detected = self.reversed_mapper.get(ext, None)
            if lang_detected:
                lang = lang_detected
        return lang
