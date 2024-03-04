import argparse
import json
import re
import sys
import urllib
from typing import Any
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import pygtrie
import ray
from data_processing.data_access import DataAccess, DataAccessFactory, DataAccessLocal
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    TransformLauncher,
)
from data_processing.transform import AbstractTableTransform
from data_processing.utils import get_logger

# from blocklist_utils import build_trie_struct, read_domain_list, reverse_url
from dotenv import load_dotenv
from ray.actor import ActorHandle


logger = get_logger(__name__)


def reverse_url(url: str) -> str:
    urllist = re.sub("[a-zA-Z]+:/+", "", url).split(".")
    urllist.reverse()
    return ".".join(urllist)


def get_domain_list(domainlist_url: str, data_access: DataAccess = None):
    domain_list = []
    if data_access is None:
        logger.info(f"Reading domain list in from {domainlist_url} as ")
        config = {"input_folder": "/tmp", "output_folder": "/tmp"}
        data_access = DataAccessLocal(config, [], False, -1)
    logger.info(f"Reading domain list from {domainlist_url} ")
    blocklist_file_dict = data_access.get_folder_files(domainlist_url)
    for file_name, file_contents in blocklist_file_dict.items():
        domains = file_contents.decode("utf-8").split("\n")
        domain_list_from_file = [domain.strip() for domain in domains if not domain.startswith("#")]
        logger.info(f"Adding {len(domain_list_from_file)} domains from {file_name}")
        domain_list += domain_list_from_file
    domain_list = set(domain_list)
    logger.info(f"Added {len(domain_list)} domains to domain list")
    return domain_list


### Configuration keys

annotation_column_name_key = "bl_annotation_column_name"
source_url_column_name_key = "bl_source_url_column_name"
blocked_domain_list_url_key = "bl_blocked_domain_list_url"
blocked_domain_list_url_default = "cos-optimal-llm-pile/spark_test/remove-cma-1/blocklists_refinedweb_subset/"
annotation_column_name_default = "url_blocklisting_refinedweb"
source_column_name_default = "title"
domain_refs_key = "__domain_refs"
""" A hidden key used by the runtime to pass a ray object reference to the transform"""


class BlockListTransform(AbstractTableTransform):
    """
    Implements blocklisting - given a set of documents and the URLs they were
    downloaded from, mark the documents that are from block listed domains
    """

    def __init__(self, config: dict):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, BlockListTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """

        super().__init__(config)
        self.blocklist_annotation_column_name = config.get(annotation_column_name_key, annotation_column_name_default)
        self.source_url_column_name = config.get(source_url_column_name_key, source_column_name_default)
        runtime_provided_domain_refs = config.get(domain_refs_key, None)
        domain_list = None

        if runtime_provided_domain_refs is not None:  # The load the blocklist here.
            try:
                domain_list = ray.get(runtime_provided_domain_refs)
            except Exception as e:
                msg1 = "Exception loading list of block listed domains from ray runtime"
                msg2 = f"Exception loading list of block listed domains from ray runtime: {e}"
                logger.error(msg2)
        if domain_list is None:
            url = config.get(blocked_domain_list_url_key, blocked_domain_list_url_default)
            if url is None:
                raise RuntimeError(f"Missing configuration value for key {annotation_column_name_key}")
            domain_list = get_domain_list(url)

        # build trie structure for block listing
        self.trie = pygtrie.StringTrie(separator=".")

        for url in domain_list:
            self.trie[reverse_url(url)] = ""
        del domain_list

    def transform(self, table: pa.Table) -> tuple[list[pa.Table], dict]:
        """
        TODO: update this description (Put Transform-specific to convert one Table to another Table.
        This implementation makes no modifications so effectively implements a copy of the input parquet to the output folder, without modification.)
        """

        def check_blocklist_trie(url):
            # to block
            url_netloc = urlparse(url).netloc
            if isinstance(
                self.trie.shortest_prefix(reverse_url(url_netloc)),
                pygtrie.Trie._Step,
            ):
                return url_netloc
            else:
                return ""

        in_df = pl.from_arrow(table)
        out_df = in_df.with_columns(
            pl.col(self.source_url_column_name)
            .map_elements(check_blocklist_trie)
            .alias(self.blocklist_annotation_column_name)
        )
        out_table = out_df.to_arrow()
        # metadata for block listing includes the total number of documents
        # and the count of block listed documents
        total_docs_count = len(out_table)
        blocklisted_docs_count = len(
            out_df.select(
                pl.col(self.blocklist_annotation_column_name).filter(
                    pl.col(self.blocklist_annotation_column_name).ne("")
                )
            )
        )
        metadata = {
            "total_docs_count": total_docs_count,
            "blocklisted_docs_count": blocklisted_docs_count,
        }
        return [out_table], metadata


class BlockListTransformConfiguration(DefaultTableTransformConfiguration):

    """
    Provides support for configuring and using the associated Transform class include
    configuration with CLI args and combining of metadata.
    """

    def __init__(self):
        super().__init__(name="blocklist", transform_class=BlockListTransform, runtime_class=BlockListRuntime)
        self.params = {}

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add Transform-specific arguments to the given parser.
        This will be included in a dictionary used to initialize the BlockListTransform.
        By convention a common prefix should be used for all mutator-specific CLI args
        (e.g, noop_, pii_, etc.)
        """
        parser.add_argument(
            f"--{blocked_domain_list_url_key}",
            type=str,
            required=False,
            default=blocked_domain_list_url_default,
            help="COS URL or local folder (file or directory) that points to the list of block listed domains.  "
            "If not running in Ray, this must be a local folder.",
        )
        parser.add_argument(
            f"--{annotation_column_name_key}",
            type=str,
            required=False,
            default=annotation_column_name_default,
            help="Name of the table column that contains the block listed domains",
        )

        parser.add_argument(
            f"--{source_url_column_name_key}",
            type=str,
            required=False,
            default=source_column_name_default,
            help="Name of the table column that has the document download URL",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        dargs = vars(args)
        self.params[blocked_domain_list_url_key] = dargs.get(blocked_domain_list_url_key)
        self.params[annotation_column_name_key] = dargs.get(annotation_column_name_key)
        self.params[source_url_column_name_key] = dargs.get(source_url_column_name_key)
        return True


class BlockListRuntime(DefaultTableTransformRuntime):
    """
    Block list runtime support
    """

    def __init__(self, params: dict[str, Any]):
        """
        Create filter runtime
        :param params: parameters, that should include
            blocklist_conf_url - the URL where the list of blocked domains is available
            blocklist_column_name - name of the block list output column
            blocklist_doc_source_url - name of the column that contains the document source
        """
        super().__init__(params)

    def get_transform_config(
        self, data_access_factory: DataAccessFactory, statistics: ActorHandle, files: list[str]
    ) -> dict[str, Any]:
        """
        Set environment for filter execution
        :param data_access_factory - data access factory
        :param statistics - reference to the statistics object
        :param files - list of files to process
        :return: dictionary of filter init params
        """
        # create the list of blocked domains by reading the files at the conf_url location
        data_access = data_access_factory.create_data_access()
        url = self.params.get(blocked_domain_list_url_key, None)
        if url is None:
            raise RuntimeError(f"Missing configuration key {blocked_domain_list_url_key}")
        domain_list = get_domain_list(url, data_access)
        domain_refs = ray.put(list(domain_list))
        logger.info(f"{domain_refs_key} = {domain_refs}")
        return {domain_refs_key: domain_refs} | self.params


# if __name__ == "__main__":
#     launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
#     launcher.launch()


if __name__ == "__main__":
    launcher = TransformLauncher(transform_runtime_config=BlockListTransformConfiguration())
    launcher.launch()

# if __name__ == "__main__":
#     # Not currently used, but shows how one might use the two classes above outside of ray.
#     dotenv_path = os.path.join(os.getenv("HOME"), ".gmf_env")
#     load_dotenv(dotenv_path=dotenv_path)
#     parser = argparse.ArgumentParser()
#     runtime = BlockListTransformRuntime()
#     runtime.add_mutator_cli_args(parser)
#     args = parser.parse_args()
#     mutator = BlockListTransform(vars(args))
#     # Sample table
#     pq_path = os.path.join(os.getenv("HOME"), "de", "blocklist.parquet")
#     df_pl = pl.read_parquet(pq_path)
#     tbl_arrow = df_pl.to_arrow()
#     out_table, meta = mutator.mutate(tbl_arrow)
#     out_df = pl.from_arrow(out_table)
#     print(out_df)
#     print(json.dumps(meta, indent=2, default=str))
