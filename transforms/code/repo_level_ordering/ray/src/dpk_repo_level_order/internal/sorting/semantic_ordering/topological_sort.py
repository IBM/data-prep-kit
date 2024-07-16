import random

import networkx as nx
import pandas as pd
from dpk_repo_level_order.internal.sorting.semantic_ordering.utils import sort_by_path
from networkx import read_graphml


# Find and remove cycles by removing random edges from cycle
def remove_cycles(g, logger=None):
    random.seed(42)
    repeat_loop = True
    edges_removed = []
    while repeat_loop:
        try:
            cycle = nx.find_cycle(g, orientation="original")
            try:
                rm_edge = min(cycle, key=lambda e: g.nodes[e[0]]["num_methods"])
            except:
                if logger:
                    logger.info("remove_cycles: Randomly removing edge")
                rm_edge = random.choice(cycle)  # select random edge

            g.remove_edge(rm_edge[0], rm_edge[1])
            edges_removed.append(rm_edge[:2])

        except nx.exception.NetworkXNoCycle:
            repeat_loop = False

    return edges_removed


# Obtain topologically sorted weakly connected components of the graph
def topo_sort_components(input_graph, logger=None):
    edges_removed = remove_cycles(input_graph, logger)
    if logger:
        logger.info(f"Number of Edges removed - {len(edges_removed)}")
        logger.info(f"Edges removed - {edges_removed}")

    # get connected components of graph
    connected_nodes = sorted(nx.weakly_connected_components(input_graph), key=len, reverse=True)
    list_of_digraphs = []
    list_of_sorted_nodes = []

    for nodes in connected_nodes:
        subgraph = nx.subgraph(input_graph, nodes)
        list_of_digraphs.append(subgraph)
        list_of_sorted_nodes.append(list(nx.topological_sort(subgraph)))

    return list_of_sorted_nodes


# Perform the topological sorting of the nodes in the input graph and
# obtain the list of nodes in topologically sorted order
def perform_topological_sort(input_graph, logger=None):
    if logger:
        logger.info(input_graph)

    n_nodes_start = input_graph.number_of_nodes()
    sorted_comps_list = topo_sort_components(input_graph, logger)

    if logger:
        logger.info(input_graph)

    nodes_without_depedencies = []
    nodes_with_dependencies = []
    for e in sorted_comps_list:
        if len(e) == 1:
            nodes_without_depedencies.extend(e)
        else:
            nodes_with_dependencies.extend(e)

    n_no_dependencies = len(nodes_without_depedencies)

    # comps_with_dependencies = sorted_comps_list[n_no_dependencies:]
    if n_no_dependencies == 0:
        comps_with_dependencies = sorted_comps_list
    else:
        comps_with_dependencies = sorted_comps_list[:-n_no_dependencies]

    n_nodes_with_dependencies = len(nodes_with_dependencies)
    n_comps_with_dependencies = len(comps_with_dependencies)

    n_nodes_end = n_no_dependencies + n_nodes_with_dependencies
    assert n_nodes_start == n_nodes_end, "Initial and final node counts don't match"

    if logger:
        logger.info(f"Number of nodes - {n_nodes_end}, components -  {len(sorted_comps_list)}")
        logger.info(
            f"Components without dependency - {n_no_dependencies}, with dependencies - {n_comps_with_dependencies}"
        )
        logger.info(f"Nodes without dependency - {n_no_dependencies}, with dependencies - {n_nodes_with_dependencies}")
        # input("Press enter")
        logger.debug("Sorted nodes in each component")
        logger.debug(sorted_comps_list)

        logger.debug("\n---------- nodes with no dependency")
        logger.debug(nodes_without_depedencies)
        logger.debug("\n---------- components with dependency")
        logger.debug(comps_with_dependencies)

    sorted_nodes = nodes_without_depedencies + nodes_with_dependencies
    return sorted_nodes, nodes_without_depedencies, nodes_with_dependencies


# Reorder the rows of dataframe based on topological sorting of files
def topological_sort_on_df(input_graph, df, logger, title_column_name="new_title"):

    sorted_nodes, nodes_without_depedencies, nodes_with_dependencies = perform_topological_sort(input_graph, logger)

    # df_topo_sorted = df.set_index('new_title').reindex(sorted_nodes).reset_index()
    # Added .copy() to following line to avoid following warning
    # SettingWithCopyWarning:
    # A value is trying to be set on a copy of a slice from a DataFrame.
    df_without_dep = df[df[title_column_name].isin(nodes_without_depedencies)].copy(deep=False)

    # order the dataframe based on default ordering
    if df_without_dep.shape[0] == 0:
        sorted_df_without_dep1 = df_without_dep.copy()
        sorted_df_without_dep2 = df_without_dep.copy()

        if logger:
            logger.debug("Found zero nodes without dependency")
    else:
        sorted_df_without_dep1, sorted_df_without_dep2 = sort_by_path(
            df_without_dep, logger, split_by_filetype=True, title_column_name=title_column_name
        )

    if logger:
        logger.debug("Sorted nodes without dependency - part1")
        logger.debug(sorted_df_without_dep1.head())
        if sorted_df_without_dep2 is not None:
            logger.debug("Sorted nodes without dependency - part2")
            logger.debug(sorted_df_without_dep2.head())

    df_with_dep = df[df[title_column_name].isin(nodes_with_dependencies)]
    sorted_df_with_dep = df_with_dep.sort_values(
        by=title_column_name,
        key=lambda column: column.map(lambda e: nodes_with_dependencies.index(e)),
    )

    # combine the two dataframe subsets to obtain final sorted dataframe
    if sorted_df_without_dep2 is not None:
        df_topo_sorted = pd.concat([sorted_df_without_dep1, sorted_df_with_dep, sorted_df_without_dep2])
    else:
        df_topo_sorted = pd.concat([sorted_df_without_dep1, sorted_df_with_dep])

    assert df_topo_sorted.shape[0] == df.shape[0], "Row counts don't match"

    return df_topo_sorted


def get_dependency_graph():
    g = read_graphml("./emerge-file_result_dependency_graph.graphml")
    g.edges()
    nodes_to_remove = [node for node in g.nodes() if not node.endswith(".py")]
    g.remove_nodes_from(nodes_to_remove)

    return g


def get_dependency_graph2(code_path, logger):
    from pathlib import Path

    from build_dep_graph import build_edges

    path = Path(code_path)
    fake_repo_name = f"A/{code_path.split('/')[-1]}"
    res = [p for p in path.rglob("*")]
    folder_to_ignore = ".git"
    res = [p for p in res if not str(p.relative_to(path)).startswith(folder_to_ignore)]
    files_dict = [
        {
            "new_title": str(p.relative_to(path)),
            "contents": p.read_text(errors="ignore"),
            "ext": f".{p.name.split('.')[-1]}",
            "repo_name": fake_repo_name,
        }
        for p in res
        if p.is_file()
    ]
    for row in files_dict:
        if row["ext"] == ".h":
            row["language"] = "C++"
        else:
            row["language"] = "None"
    import pandas as pd

    files_df = pd.DataFrame(files_dict)
    dep_graph = build_edges(files_df, logger)
    logger.debug(dep_graph.edges)
    return files_df, dep_graph
