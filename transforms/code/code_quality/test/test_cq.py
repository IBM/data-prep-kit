import pyarrow as pa
import pyarrow.parquet as pq
from cq_transform import CodeQualityTransform, CodeQualityTransformConfiguration
from data_processing.data_access import DataAccessLocal
from data_processing.ray.transform_runtime import get_transform_config
from data_processing.test_support.transform import AbstractTransformTest
from data_processing.utils import ParamsUtils


class TestCodeQualityTransform(AbstractTransformTest):
    def get_test_transform_fixtures(self) -> list[tuple]:
        cli = [
            "--cq_contents_column_name",
            "contents",
            "--cq_language_column_name",
            "language",
            "--cq_tokenizer",
            "codeparrot/codeparrot",
        ]

        # Use the CodeQualityTransformConfiguration to compute the config parameters
        cqconfig = CodeQualityTransformConfiguration()
        config = get_transform_config(cqconfig, cli)

        fixtures = [
            (
                CodeQualityTransform(config),
                [self.input_table],
                [self.expected_output_table],
                [{}, {}],
            ),
        ]
        return fixtures

    input_table = pq.ParquetFile("../test-data/input/sample_1.parquet").read()
    expected_output_table = pq.ParquetFile("../test-data/expected/sample_1.parquet").read()
