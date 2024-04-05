import os
from fdedup_transform import FdedupTableTransformConfiguration
from data_processing.test_support.ray import AbstractTransformLauncherTest


class TestRayBlocklistTransform(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../test-data"))
        config = {
            # When running in ray, our Runtime's get_transform_config() method  will load the domains using
            # the orchestrator's DataAccess/Factory. So we don't need to provide the bl_local_config configuration.
            # columns used
            "doc_column": "contents",
            "id_column": "int_id_column",
            "cluster_column": "cluster",
            # infrastructure
            "bucket_cpu": 0.5,
            "doc_cpu": 0.5,
            "mhash_cpu": 0.5,
            "num_doc_actors": 2,
            "num_bucket_actors": 1,
            "num_minhash_actors": 1,
            "num_preprocessors": 2,
            "num_permutations": 64,
            # fuzzy parameters
            "threshold": 0.8,
            "shingles_size": 5,
            "japanese_data": False,
            "delimiters": " ",
            # Random delay between reads
            "random_delay_limit": 5,
            # snapshotting
            "snapshot_delay": 1,
            "use_doc_snapshot": False,
            "use_bucket_snapshot": False,
        }
        fixtures = [(FdedupTableTransformConfiguration(), config, basedir + "/input", basedir + "/expected")]
        return fixtures
