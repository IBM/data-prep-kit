import argparse
import json
import re
import sys
from typing import Any
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import pygtrie
import ray
from data_processing.data_access import DataAccessFactory
from data_processing.ray import (
    DefaultTableTransformConfiguration,
    DefaultTableTransformRuntime,
    RayUtils,
    TransformLauncher,
)
from data_processing.ray.transform_runtime import DefaultTableTransformConfiguration
from data_processing.transform import AbstractTableTransform
from data_processing.utils import GB, TransformUtils

# from blocklist_utils import build_trie_struct, read_domain_list, reverse_url
from dotenv import load_dotenv
from ray.actor import ActorHandle


def reverse_url(url: str) -> str:
    urllist = re.sub("[a-zA-Z]+:/+", "", url).split(".")
    urllist.reverse()
    return ".".join(urllist)


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
        self.blocklist_column_name = config.get("blocklist_column_name")
        self.blocklist_source_url = config.get("blocklist_doc_source_url")

        # get list of blocked domains from the driver
        try:
            self.domain_list = ray.get(config.get("domain_refs1", []))
            if not self.domain_list:
                self.domain_list = config.get("blocklist_local_domain_list", [])
                print("Getting domain_list from local config")
        except Exception as e:
            print(f"Exception {e} loading list of blocklisted domains")
            sys.exit(1)

        # build trie structure for block listing
        self.trie = pygtrie.StringTrie(separator=".")

        for url in self.domain_list:
            self.trie[reverse_url(url)] = ""
        del self.domain_list

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
            pl.col(self.blocklist_source_url).map_elements(check_blocklist_trie).alias(self.blocklist_column_name)
        )
        out_table = out_df.to_arrow()
        # metadata for block listing includes the total number of documents
        # and the count of block listed documents
        total_docs_count = len(out_table)
        blocklisted_docs_count = len(
            out_df.select(pl.col(self.blocklist_column_name).filter(pl.col(self.blocklist_column_name).ne("")))
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
            "--blocklist_conf_url",
            type=str,
            required=False,
            default="cos-optimal-llm-pile/spark_test/remove-cma-1/blocklists_refinedweb_subset/",
            help="COS URL (file or directory) that points to the list of block listed domain",
        )
        parser.add_argument(
            "--blocklist_column_name",
            type=str,
            required=False,
            default="url_blocklisting_refinedweb",
            help="name of the table column that contains the block listed domains",
        )
        parser.add_argument(
            "--blocklist_doc_source_url",
            type=str,
            required=False,
            default="title",
            help="name of the table column that has the document download URL",
        )

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        self.params["blocklist_conf_url"] = args.blocklist_conf_url
        self.params["blocklist_column_name"] = args.blocklist_column_name
        self.params["blocklist_doc_source_url"] = args.blocklist_doc_source_url
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
        blocklist_file_dict = data_access.get_folder_files(self.params["blocklist_conf_url"])
        domain_list = []
        for file_name, file_contents in blocklist_file_dict.items():
            domains = file_contents.decode("utf-8").split("\n")
            domain_list_from_file = [domain.strip() for domain in domains if not domain.startswith("#")]
            print(f"Adding {len(domain_list_from_file)} domains from {file_name}")
            domain_list += domain_list_from_file
        domain_list = set(domain_list)
        print(f"Added {len(domain_list)} domains to domain list")
        domain_refs = ray.put(list(domain_list))
        print(f"domain_refs = {domain_refs}")
        return {"domain_refs": domain_refs} | self.params


# if __name__ == "__main__":
#     launcher = TransformLauncher(transform_runtime_config=NOOPTransformConfiguration())
#     launcher.launch()


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
