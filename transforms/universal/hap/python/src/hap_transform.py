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
import torch, nltk
import pandas as pd
import pyarrow as pa
from typing import Any
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import GB, TransformUtils, get_logger
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from argparse import ArgumentParser, Namespace
device = "cuda:0" if torch.cuda.is_available() else "cpu"
nltk.download('punkt_tab')

class HAPTransform(AbstractTableTransform):
    """
    Implements HAP transform
    """

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.model_name_or_path = config.get("model_name_or_path", "ibm-granite/granite-guardian-hap-38m")
        self.annotation_column = config.get("annotation_column", "hap_score")
        self.doc_text_column = config.get("doc_text_column", "contents")
        self.max_length = config.get("max_length", 512)
        self.batch_size = config.get("batch_size", 128)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name_or_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name_or_path)

    def _apply_model(self, data: list, batch_size: int) -> list[float]:
        num_batches = len(data) // batch_size
        data_sent_scores = []
        for i in range(num_batches + 1):
            print(f"Processing batch: {i}/{num_batches}")
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            if start_idx >= end_idx: continue
            inputs = self.tokenizer(data[start_idx:end_idx], max_length=self.max_length, padding=True, truncation=True, return_tensors="pt").to(device)
            with torch.no_grad():
                logits = self.model(**inputs).logits
                data_sent_scores.extend(torch.softmax(logits, dim=1).cpu().numpy()[:, 1].tolist())
        return data_sent_scores 

    def _apply_sent_split(self, data: list) -> tuple[list[str], list[int]]:
        data_sents, data_sent_ids = [], []
        for i, e in enumerate(data):
            s_list = nltk.sent_tokenize(e)
            data_sents.extend(s_list)
            data_sent_ids.extend([i] * len(s_list))
        return data_sents, data_sent_ids
        
    def _apply_aggregate(self, nb_doc: int, sent_scores: list[float], sent_ids: list[int]) -> list[float]:
        doc_scores = []
        for i in range(nb_doc):
            temp = [score for idx, score in zip(sent_ids, sent_scores) if i == idx]
            doc_scores.append(max(temp))
        return doc_scores
                 
    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Process a table of document text to generate a hap score for each document
        :param table: Pyarrow table
        :return: a table with an additional hap_score column
        """
        # make sure that the table contains "contents" column
        TransformUtils.validate_columns(table=table, required=[self.doc_text_column])
        self.df = table.to_pandas()
        df_doc_list = []
        for i in range(len(self.df)):
            text = self.df.iloc[i][self.doc_text_column]
            text = " ".join(text.strip().splitlines())
            df_doc_list.append(text)
        
        data_sents, data_sent_ids = self._apply_sent_split(df_doc_list)
        data_sent_scores = self._apply_model(data_sents, self.batch_size)
        
        df_doc_scores = self._apply_aggregate(len(df_doc_list), data_sent_scores, data_sent_ids)
        assert len(df_doc_list) == len(df_doc_scores)
        
        self.df['hap_score'] = df_doc_scores
        print(self.df)
        
        out_table = pa.Table.from_pandas(self.df)
        metadata = {}
        return [out_table], metadata


logger = get_logger(__name__)
class HAPTransformConfiguration(TransformConfiguration):
    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="hap", transform_class=HAPTransform)
        self.params = {}
        self.daf = None

    def add_input_params(self, parser: ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the HAPTransform.
        By convention a common prefix should be used for all transform-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            "--model_name_or_path",
            type=str,
            required=False,
            default="ibm-granite/granite-guardian-hap-38m",
            help="HAP model path",
        )

        parser.add_argument(
            "--annotation_column",
            type=str,
            required=False,
            default="hap_score",
            help="hap score for each document",
        )

        parser.add_argument(
            "--doc_text_column",
            type=str,
            required=False,
            default="contents",
            help="The column name that contains the document text",
        )
        
        parser.add_argument(
            "--inference_engine",
            type=str,
            required=False,
            default="CPU",
            help="inference engine used",
        )
        
        parser.add_argument(
            "--max_length",
            type=int,
            required=False,
            default=512,
            help="inference engine used",
        )
        
        parser.add_argument(
            "--batch_size",
            type=int,
            required=False,
            default=128,
            help="batch size",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        self.params["model_name_or_path"] = args.model_name_or_path
        self.params["annotation_column"] = args.annotation_column
        self.params["doc_text_column"] = args.doc_text_column
        self.params["inference_engine"] = args.inference_engine
        self.params["max_length"] = args.max_length
        self.params["batch_size"] = args.batch_size
        logger.info(f"hap params are {self.params} ")
        return True
