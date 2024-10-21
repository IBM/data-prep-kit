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

from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig


class UnsupoortedOperatorException(Exception):
    """Exception raised for unsupported operators."""

    pass


class PIIAnonymizer:
    def __init__(self, operator="replace"):
        self.anonymizer = AnonymizerEngine()
        self._is_valid_operator(operator)
        self.operator_config = {"DEFAULT": OperatorConfig(operator, None)}

    def _is_valid_operator(self, operator):
        valid_operators = {"replace", "redact"}
        if operator not in valid_operators:
            raise UnsupoortedOperatorException(
                f"{operator} is not supported for anonymizing.replace or redact is the " f"supported operators"
            )

        return True

    def anonymize_text(self, text, analyze_results):
        return self.anonymizer.anonymize(text, analyze_results, operators=self.operator_config)
