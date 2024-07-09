from perplexity_models import PerplexityModel
from evaluate import load
import pyarrow as pa

class TransformerModel(PerplexityModel):
    def __init__(self, path: str):
        self.path = path
        self.perplexity = load("perplexity", module_type="metric")
    
    def get_perplexities_raw(self, texts: pa.ChunkedArray) -> list:
        return self.perplexity.compute(predictions=texts.to_pylist(), model_id=self.path)['perplexities']