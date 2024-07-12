import logging
import re
from collections import defaultdict, namedtuple
from logging import Logger
from pathlib import Path

import networkx as nx
import pandas as pd
from emerge.analysis import Analysis
from emerge.appear import Emerge
from emerge.files import FileScanMapper
from emerge.graph import GraphType
from emerge.languages.abstractparser import CoreParsingKeyword
from emerge.languages.javascriptparser import JavaScriptParsingKeyword
from emerge.languages.typescriptparser import TypeScriptParsingKeyword


regex_patters_dict = {
    "JAVA_PARSER": r"\b(?!if|for|while|switch|catch)\b[a-zA-Z\d_]+?\s*?\([a-zA-Z\d\s_,\>\<\?\*\.\[\]]*?\)\s*?\{",
    "KOTLIN_PARSER": r"fun\s[a-zA-Z\d_\.]+?\s*?\([a-zA-Z\d\s_,\?\@\>\<\?\*\.\[\]\:]*?\)\s*?.*?(\{|\=)",
    "OBJC_PARSER": r"[\-\+]\s*?[a-zA-Z\d_\(\)\:\*\s]+?\s*?\{",
    "SWIFT_PARSER": r"func\s*?[a-zA-Z\d_\(\)\:\*\s\-\<\>\?\,\[\]\.]+?\s*?\{",
    "RUBY_PARSER": r"(def)\s(.+)",
    "GROOVY_PARSER": r"\b(?!if|for|while|switch|catch)\b[a-zA-Z\d_]+?\s*?\([a-zA-Z\d\s_,\>\<\?\*\.\[\]\=\@\']*?\)\s*?\{",
    "JAVASCRIPT_PARSER": r"(function\s+?)([a-zA-Z\d_\:\*\-\<\>\?\,\[\]\.\s\|\=\$]+?)\(([a-zA-Z\d_\(\)\:\*\s\-\<\>\?\,\[\]\.\|\=\$\/]*?)\)*?[\:]*?\s*?\{",
    "TYPESCRIPT_PARSER": r"(function\s+?)([a-zA-Z\d_\:\*\-\<\>\?\,\[\]\.\s\|\=\$]+?)\(([a-zA-Z\d_\(\)\:\*\s\-\<\>\?\,\[\]\.\|\=\$\/]*?)\)*?[\:]*?\s*?\{",
    "C_PARSER": r"\b(?!if|for|while|switch)\b[a-zA-Z\d_]+?\s*?\([a-zA-Z\d\s_,\*]*?\)\s*?\{",
    "CPP_PARSER": r"\b(?!if|for|while|switch)\b[a-zA-Z\d\_\:\<\>\*\&]+?\s*?\([\(a-zA-Z\d\s_,\*&:]*?\)\s*?\w+\s*?\{",
    "PYTHON_PARSER": r"(def)\s.+(.+):",
    "GO_PARSER": r"func\s*?[a-zA-Z\d_\(\)\:\*\s\-\<\>\?\,\[\]\.]+?\s*?\{",
}

compiled_regex_dict = {key: re.compile(val) for key, val in regex_patters_dict.items()}


def get_custom_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    consoleHandler = logging.StreamHandler()
    # formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:%(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    return logger


def get_analysis_obj(full_repo_name):
    analysis = Analysis()
    analysis.source_directory = f"/{full_repo_name.split('/')[-1]}/"
    return analysis


def get_parser(file_extension, language):
    parsers = Emerge()._parsers
    only_permit_languages = None
    if language == "c":
        only_permit_languages = ["c"]
    elif language in ["c++", "cpp"]:
        only_permit_languages = ["cpp"]
    parser_name = FileScanMapper.choose_parser(file_extension, only_permit_languages=only_permit_languages)
    if parser_name:
        parser = parsers[parser_name]
    else:
        parser = None
    return parser


def get_parsers(files_df):
    ext_parsers_map = {}
    for ext, language in files_df[["ext", "language"]].value_counts().index.values:
        ext_parsers_map[(ext, language)] = get_parser(ext, language)
    return ext_parsers_map


def get_parser_results(
    files,
    analysis,
    ext_parsers_map,
    files_set,
    logger: Logger,
    title_column_name,
    extension_column_name="ext",
):
    unparsed_files = []
    parser_results = {}
    for file_dict in files:
        full_file_name = file_dict[title_column_name]
        try:
            file_name = Path(full_file_name).name
            file_content = file_dict["contents"]
            parser = ext_parsers_map[(file_dict[extension_column_name], file_dict["language"])]
            if parser:
                parser.generate_file_result_from_analysis(
                    analysis,
                    file_name=file_name,
                    full_file_path=full_file_name,
                    file_content=file_content,
                )
            else:
                unparsed_files.append(full_file_name)
        except:
            logger.exception(f"method - get_parser_results: parser finding dependencies failed for {full_file_name}")
            unparsed_files.append(full_file_name)
    logger.info(f"Unparsed files - {len(unparsed_files)} out of total {len(files)} files")
    for ext, language in ext_parsers_map:
        parser = ext_parsers_map[(ext, language)]
        if parser:
            resolve_dependencies(ext, parser, analysis, files_set)
            parser.after_generated_file_results(analysis)
            parser_results.update(parser.results)
    return parser_results


def get_files_list(files_df, title_column_name):
    return list(files_df[title_column_name].unique())


def find_file_dependencies(
    files_set: set[str],
    file_name: str,
    scanned_import_dependencies: list[str],
    analysis,
):
    file_deps = []
    if file_name.endswith(".py"):
        for dep in scanned_import_dependencies:
            if not dep.endswith(".py"):
                dep += ".py"
            if not dep.startswith("/"):
                dep = "/" + dep
            if dep in files_set:
                file_deps.append(dep)
    elif file_name.endswith(".java"):
        for dep in scanned_import_dependencies:
            dep_file = dep.replace(".", "/") + ".java"
            for exst_file in files_set:
                if dep_file in exst_file:
                    file_deps.append(exst_file)
    else:
        # ".c", ".cpp", ".js", ".jsx", ".ts", ".tsx", ".go", ".h", ".m", ".swift", ".kt", ".groovy", ".rb"
        for dep in scanned_import_dependencies:
            if dep in files_set:
                file_deps.append(dep)
    return file_deps


def remove_tmp_src_dir(file_name, analysis):
    return file_name[len(analysis.source_directory) :]


def find_num_methods(scanned_parser_name, scanned_tokens):
    full_string = " ".join(scanned_tokens)
    find_method_expression = compiled_regex_dict[scanned_parser_name]
    return len(find_method_expression.findall(full_string))


def build_graph_from_results(parser_results, files_set, files_list, analysis, title_column_name):
    dep_graph = nx.DiGraph()
    for file_name in files_list:
        file_name = remove_tmp_src_dir(file_name, analysis)
        dep_graph.add_node(file_name)
    for file_name in parser_results:
        file_result = parser_results[file_name]
        scanned_import_dependencies = file_result.scanned_import_dependencies
        file_deps = find_file_dependencies(files_set, file_name, scanned_import_dependencies, analysis)
        file_name = remove_tmp_src_dir(file_name, analysis)
        num_methods = find_num_methods(file_result.scanned_by, file_result.scanned_tokens)
        dep_graph.nodes[file_name]["num_methods"] = num_methods
        for dep in file_deps:
            dep = remove_tmp_src_dir(dep, analysis)
            if dep != file_name:
                dep_graph.add_edge(dep, file_name)
    return dep_graph


def try_resolve_js_jsx_dependency(parser, dependency, result, analysis, files_set):
    # check if there are any configured dependency substrings to be replaced directly, e.g. '@scope/sub/path' -> src/sub/path
    if analysis.import_aliases_available:
        renamed_dependency = parser.replace_substring_if_any_mapping_key_in_string_exists(
            dependency, analysis.import_aliases
        )
        if renamed_dependency != dependency:
            dependency = renamed_dependency

    # check for module identifiers (@)
    if CoreParsingKeyword.AT.value in dependency:
        # check if a module identifier with a @scope + subpath combination physically exist,
        # e.g. '@scope/sub/path' (https://nodejs.org/api/modules.html#modules_all_together, LOAD_PACKAGE_EXPORTS)
        if CoreParsingKeyword.SLASH.value in dependency:
            subpath = "/".join(dependency.split("/")[1:])
            check_package_index_export = f"{subpath}/index.js"
            check_package_subpath_import = f"{subpath}.js"

            # check if there is a package index .ts file
            if check_package_index_export in files_set:
                dependency = parser.create_relative_analysis_file_path(
                    analysis.source_directory, check_package_index_export
                )
            # check if the subpath exists as a .ts file
            if check_package_subpath_import in files_set:
                dependency = parser.create_relative_analysis_file_path(
                    analysis.source_directory, check_package_subpath_import
                )

        # pylint: disable=unnecessary-pass
        pass  # otherwise let the module @-dependency as it is

    # check for index.js imports (https://nodejs.org/api/modules.html#modules_all_together)
    elif dependency == CoreParsingKeyword.DOT.value:
        index_dependency = dependency.replace(CoreParsingKeyword.DOT.value, "./index.js")
        index_dependency = parser.resolve_relative_dependency_path(
            index_dependency, str(result.absolute_dir_path), analysis.source_directory
        )
        check_dependency_path = f"{index_dependency}"
        if check_dependency_path in files_set:  # check if the resolved index_dependency exists, then modify
            dependency = f"{index_dependency}"

    elif (
        dependency.count(CoreParsingKeyword.POSIX_CURRENT_DIRECTORY.value) == 1
        and JavaScriptParsingKeyword.PARENT_DIRECTORY.value not in dependency
    ):  # e.g. ./foo
        dependency = dependency.replace(CoreParsingKeyword.POSIX_CURRENT_DIRECTORY.value, "")

        # adjust dependency to have a relative analysis path
        dependency = parser.create_relative_analysis_path_for_dependency(
            dependency, str(result.relative_analysis_path)
        )

    elif (
        JavaScriptParsingKeyword.PARENT_DIRECTORY.value in dependency
    ):  # contains at least one relative parent element '../
        dependency = parser.resolve_relative_dependency_path(
            dependency, str(result.absolute_dir_path), analysis.source_directory
        )

    # check and verify if we need to add a remaining .js suffix
    check_dependency_path = f"{dependency}.js"
    if Path(dependency).suffix != ".js" and check_dependency_path in files_set:
        dependency = f"{dependency}.js"

    # check if the dependency maybe results from an index.js import
    check_dependency_path_for_index_file = f"{dependency}/index.js"
    if check_dependency_path_for_index_file in files_set:
        dependency = f"{dependency}/index.js"

    return dependency


def try_resolve_ts_tsx_dependency(parser, dependency, result, analysis, files_set):
    if analysis.import_aliases_available:
        renamed_dependency = parser.replace_substring_if_any_mapping_key_in_string_exists(
            dependency, analysis.import_aliases
        )
        if renamed_dependency != dependency:
            dependency = renamed_dependency

    # check for module identifiers (@)
    if CoreParsingKeyword.AT.value in dependency:

        # check if a module identifier with a @scope + subpath combination physically exist,
        # e.g. '@scope/sub/path' (https://nodejs.org/api/modules.html#modules_all_together, LOAD_PACKAGE_EXPORTS)
        if CoreParsingKeyword.SLASH.value in dependency:
            subpath = "/".join(dependency.split("/")[1:])
            check_package_index_export = f"{subpath}/index.ts"
            check_package_subpath_import = f"{subpath}.ts"

            # check if there is a package index .ts file
            if check_package_index_export in files_set:
                dependency = parser.create_relative_analysis_file_path(
                    analysis.source_directory, check_package_index_export
                )
            # check if the subpath exists as a .ts file
            if check_package_subpath_import in files_set:
                dependency = parser.create_relative_analysis_file_path(
                    analysis.source_directory, check_package_subpath_import
                )

        # pylint: disable=unnecessary-pass
        pass  # otherwise let the module @-dependency as it is

    elif (
        dependency.count(CoreParsingKeyword.POSIX_CURRENT_DIRECTORY.value) == 1
        and TypeScriptParsingKeyword.PARENT_DIRECTORY.value not in dependency
    ):  # e.g. ./foo
        dependency = dependency.replace(CoreParsingKeyword.POSIX_CURRENT_DIRECTORY.value, "")

        # adjust dependency to have a relative analysis path
        dependency = parser.create_relative_analysis_path_for_dependency(
            dependency, str(result.relative_analysis_path)
        )

    elif (
        TypeScriptParsingKeyword.PARENT_DIRECTORY.value in dependency
    ):  # contains at lease one relative parent element '..'
        dependency = parser.resolve_relative_dependency_path(
            dependency, str(result.absolute_dir_path), analysis.source_directory
        )

    # verify if the dependency physically exist, then add the remaining suffix
    check_dependency_path = f"{dependency}.ts"
    if check_dependency_path in files_set:
        dependency = f"{dependency}.ts"

    # check if the dependency maybe results from an index.ts import
    check_dependency_path_for_index_file = f"{dependency}/index.ts"
    if check_dependency_path_for_index_file in files_set:
        dependency = f"{dependency}/index.ts"

    return dependency


def try_resolve_rb_dependency(parser, dependency, result, analysis, files_set):

    successfully_resolved_dependency = False

    # resolve in pure POSIX way
    resolved_posix_dependency = parser.resolve_relative_dependency_path(
        dependency, str(result.absolute_dir_path), analysis.source_directory
    )
    if ".rb" not in resolved_posix_dependency:
        resolved_posix_dependency = f"{resolved_posix_dependency}.rb"

    check_dependency_path = f"{resolved_posix_dependency}"
    if check_dependency_path in files_set:
        dependency = resolved_posix_dependency
        successfully_resolved_dependency = True

    if not successfully_resolved_dependency:
        # otherwise try to resolve it as a non-POSIX dependency, i.e. where "../" imports from the current directory "./"
        non_posix_dependency = ""
        resolved_non_posix_dependency = ""

        # resolve/check by reducing only the first ".." to "."
        if CoreParsingKeyword.POSIX_PARENT_DIRECTORY.value in dependency:
            non_posix_dependency = dependency.replace(
                CoreParsingKeyword.POSIX_PARENT_DIRECTORY.value,
                CoreParsingKeyword.POSIX_CURRENT_DIRECTORY.value,
                1,
            )
            resolved_non_posix_dependency = parser.resolve_relative_dependency_path(
                non_posix_dependency,
                str(result.absolute_dir_path),
                analysis.source_directory,
            )
            if ".rb" not in resolved_non_posix_dependency:
                resolved_non_posix_dependency = f"{resolved_non_posix_dependency}.rb"

            check_dependency_path = f"{resolved_non_posix_dependency}"
            if check_dependency_path in files_set:
                dependency = resolved_non_posix_dependency
                successfully_resolved_dependency = True

    # as a last step, try to check if the dependency can be found in a local "lib" folder
    if not successfully_resolved_dependency:

        resolved_lib_dependency = parser.resolve_relative_dependency_path(
            f"lib/{dependency}.rb", analysis.source_directory, analysis.source_directory
        )
        # f"{Path(analysis.source_directory)}/lib/{dependency}.rb"
        check_resolved_lib_dependency_path = f"{resolved_lib_dependency}"

        if check_resolved_lib_dependency_path in files_set:
            dependency = resolved_lib_dependency
            successfully_resolved_dependency = True

    return dependency


def try_resolve_c_cpp_objc_dependency(parser, dependency, result, analysis, files_set):
    resolved_dependency = parser.resolve_relative_dependency_path(
        dependency, str(result.absolute_dir_path), analysis.source_directory
    )
    if resolved_dependency in files_set:
        dependency = resolved_dependency
    return dependency


def resolve_dependencies(ext, parser, analysis, files_set):
    if ext in [".js", ".jsx"]:
        for file_name in parser.results:
            result = parser.results[file_name]
            scanned_import_dependencies = []
            for dependency in result.scanned_import_dependencies:
                dependency_new = try_resolve_js_jsx_dependency(parser, dependency, result, analysis, files_set)
                scanned_import_dependencies.append(dependency_new)
            result.scanned_import_dependencies = scanned_import_dependencies
    elif ext in [".ts", ".tsx"]:
        for file_name in parser.results:
            result = parser.results[file_name]
            scanned_import_dependencies = []
            for dependency in result.scanned_import_dependencies:
                dependency_new = try_resolve_ts_tsx_dependency(parser, dependency, result, analysis, files_set)
                scanned_import_dependencies.append(dependency_new)
            result.scanned_import_dependencies = scanned_import_dependencies
    elif ext in [".c", ".cpp", ".h", ".m"]:
        for file_name in parser.results:
            result = parser.results[file_name]
            scanned_import_dependencies = []
            for dependency in result.scanned_import_dependencies:
                dependency_new = try_resolve_c_cpp_objc_dependency(parser, dependency, result, analysis, files_set)
                scanned_import_dependencies.append(dependency_new)
            result.scanned_import_dependencies = scanned_import_dependencies
    elif ext == ".rb":
        for file_name in parser.results:
            result = parser.results[file_name]
            scanned_import_dependencies = []
            for dependency in result.scanned_import_dependencies:
                dependency_new = try_resolve_rb_dependency(parser, dependency, result, analysis, files_set)
                scanned_import_dependencies.append(dependency_new)
            result.scanned_import_dependencies = scanned_import_dependencies


def add_repo_path(files_df_org: pd.DataFrame, analysis, title_column_name="new_title"):
    files_df = files_df_org.copy(deep=True)
    files_df[title_column_name] = analysis.source_directory + files_df[title_column_name]
    return files_df


def add_files_info_to_analysis_obj(analysis, files_list):
    analysis.absolute_scanned_file_names = files_list
    scanned_files_nodes_in_directories = defaultdict(list)
    for file_name in files_list:
        file_folder = str(Path(file_name).parent)
        scanned_files_nodes_in_directories[file_folder].append(file_name)
    analysis.scanned_files_nodes_in_directories = scanned_files_nodes_in_directories
    nodes = {k: {"directory": True} for k in scanned_files_nodes_in_directories}
    Digraph = namedtuple("Digraph", "nodes")
    digraph = Digraph(nodes)
    GraphObj = namedtuple("GraphObj", "digraph")
    graph_obj = GraphObj(digraph)
    analysis.graph_representations[GraphType.FILESYSTEM_GRAPH.name.lower()] = graph_obj
    return analysis


def build_edges(files_df_org, logger: Logger, title_column_name="new_title"):
    full_repo_name = files_df_org.repo_name.to_list()[0]
    analysis = get_analysis_obj(full_repo_name)
    files_df = add_repo_path(files_df_org, analysis, title_column_name)
    files_df.ext = files_df.ext.str.lower()
    files_df.language = files_df.language.str.lower()
    files = files_df.to_dict(orient="records")
    files_list = get_files_list(files_df, title_column_name)
    files_set = set(files_list)
    logger.info(f"method - build_edges: Number of files received - {len(files_list)}")
    analysis = add_files_info_to_analysis_obj(analysis, files_list)
    ext_parsers_map = get_parsers(files_df)
    parser_results = get_parser_results(files, analysis, ext_parsers_map, files_set, logger, title_column_name)
    dep_graph = build_graph_from_results(parser_results, files_set, files_list, analysis, title_column_name)
    logger.info(f"Number of nodes in dependency graph - {len(dep_graph.nodes)}")
    return dep_graph


def main():
    from pathlib import Path

    # code_path = "/dccstor/shanmukh/emerge_test/test_repos/cJSON"
    # code_path = "/dccstor/vaibhav1/wisdom/basemodel/test_repos/featuretools"
    code_path = "/dccstor/shanmukh/repos/dart"
    path = Path(code_path)
    fake_repo_name = f"A/{code_path.split('/')[-1]}"
    res = [p for p in path.rglob("*")]
    folder_to_ignore = ".git"
    res = [p for p in res if not str(p.relative_to(path)).startswith(folder_to_ignore)]
    files_dict = [
        {
            "new_title": str(p.relative_to(path)),
            "contents": p.read_text(errors="ignore"),
            "ext": p.suffix,
            "repo_name": fake_repo_name,
        }
        for p in res
        if p.is_file()
    ]
    for row in files_dict:
        if row["ext"] == ".h":
            row["language"] = "C"
        else:
            row["language"] = "None"
    import pandas as pd

    files_df = pd.DataFrame(files_dict)
    dep_graph = build_edges(files_df, get_custom_logger())
    print(dep_graph.edges, len(dep_graph.edges))


if __name__ == "__main__":
    main()
