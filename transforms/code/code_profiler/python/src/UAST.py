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

import json
import networkx
import matplotlib.pyplot as plt

class UASTNode:
    """
    Represents a node in the Universal Abstract Syntax Tree (UAST).

    Attributes:
        id (int): The unique identifier of the node.
        code_snippet (str): The line(s) of code associated with the node.
        node_type (str): The type of the node.
        parents (list): The list of parent nodes.
        children (list): The list of child nodes.
        metadata (dict): The associated information/metadata of the node
        start_point (tuple(int, int)): The start line number and byte of the line of the node.
        end_point (tuple(int, int)): The end line number and byte of the node.
    """

    def __init__(self, 
                 id: int = 0,
                 code_snippet: str = None,
                 node_type: str = None,
                 parents: list = list(),
                 children: list = list(), 
                 metadata : dict = dict(),
                 start_point : tuple[int,int] = (None, None),
                 end_point : tuple[int,int] = (None, None)) -> None: 
        
        self.id = id
        self.code_snippet = code_snippet
        self.node_type = node_type
        self.parents = parents
        self.children = children
        self.metadata = metadata
        self.start_point = start_point
        self.end_point = end_point

    def __str__(self) -> str:
        return f"ID: {self.id}, Type: {self.node_type}, Snippet: {repr(self.code_snippet)}, Parents: {self.parents}, Children: {self.children}, Metadata = {self.metadata}"
    
    def __repr__(self) -> str:
        return f"ID: {self.id}, Type: {self.node_type}, Snippet: {repr(self.code_snippet)}, Parents: {self.parents}, Children: {self.children}, Metadata = {self.metadata}"
    
    def __eq__(self, other) -> bool:
        return self.id == other.id and self.code_snippet == other.code_snippet and self.node_type == other.node_type and self.parents == other.parents and self.children == other.children and self.metadata == other.metadata and self.start_point == other.start_point and self.end_point == other.end_point
    
class UASTEdge:
    """
    Represents an edge in the UAST (Universal Abstract Syntax Tree).

    Attributes:
        start_id (int): The ID of the starting node of the edge.
        end_id (int): The ID of the ending node of the edge.
        directed_relation (str): The directed relation between the nodes.
        metadata (dict): The metadata information associated with the edge.
    """

    def __init__(self, 
                 start_id: int = None, 
                 end_id: int = None, 
                 directed_relation: str = None,
                 metadata : dict = dict()):

        self.start_id = start_id
        self.end_id = end_id
        self.directed_relation = directed_relation
        self.metadata = metadata

    def __str__(self) -> str:
        return f"Start: {self.start_id}, End: {self.end_id}, Relation: {self.directed_relation}, Metadata = {self.metadata}, Metadata: {self.metadata}"
    
    def __repr__(self) -> str:
        return f"Start: {self.start_id}, End: {self.end_id}, Relation: {self.directed_relation}, Metadata = {self.metadata}, Metadata: {self.metadata}"
    
    def __eq__(self, other) -> bool:
        return self.start_id == other.start_id and self.end_id == other.end_id and self.directed_relation == other.directed_relation and self.metadata == other.metadata
    
    def __hash__(self) -> int:
        return hash((self.start_id, self.end_id, self.directed_relation, self.metadata))
    
class UAST:
    """
    Represents a graph of a Universal Abstract Syntax Tree (UAST).

    Attributes:
        nodes (dict[int, UASTNode]): A dictionary mapping node IDs to UASTNode objects.
        edges (list[UASTEdge]): A list of UASTEdge objects representing the edges between nodes.
        assigned_id (int): The ID to be assigned to the next node added to the UAST.

    Methods:
        __init__(): Initializes an empty UAST object.
        __len__(): Returns the number of nodes in the UAST.
        __str__(): Returns a string representation of the UAST.
        __repr__(): Returns a string representation of the UAST.
        __eq__(other): Checks if the UAST is equal to another UAST.
        add_node(node): Adds a node to the UAST.
        _create_root(): Creates a root node for the UAST.
        create_node(node_type, code_snippet, start_point, end_point): Creates a new node and adds it to the UAST, also returns the node object.
        add_edge(node1, node2, directed_relation, metadata): Adds an edge between two nodes in the UAST.
        get_node(id): Retrieves a node from the UAST based on its ID.
        get_nodes_of_type(node_type): Retrieves the ID of all nodes of the input type
        get_children(node): Retrieves the children of a node in the UAST.
        get_parents(node): Retrieves the parent of a node in the UAST.
        print_graph(id): Prints the UAST starting from the specified node ID.
        save_to_file(file_path): Saves the UAST to a file in JSON format.
        load_from_file(file_path): Loads the UAST from a file in JSON format.
        visualize(): Visualizes the graph using NetworkX
    """
    def __init__(self):
        self.nodes : dict[int,UASTNode] = dict()
        self.edges : list[UASTEdge] = list()
        self.assigned_id : int = 0
        self.nodes_of_type : dict = dict()
        self.root = self._create_root()

    def __len__(self) -> int:
        return len(self.nodes)

    def __str__(self) -> str:
        return f"Nodes: {self.nodes} \nEdges: {self.edges}"
    
    def __repr__(self) -> str:
        return f"Nodes: {self.nodes} \nEdges: {self.edges}"
    
    def __eq__(self, other) -> bool:
        return self.nodes == other.nodes and self.edges == other.edges

    def add_node(self, node : UASTNode) -> None:
        self.nodes[self.assigned_id] = node
        self.assigned_id += 1
        if node.node_type not in self.nodes_of_type :
            self.nodes_of_type[node.node_type] = list()
        self.nodes_of_type[node.node_type].append(node.id)
        return

    def _create_root(self) -> UASTNode:
        return self.create_node(node_type = "uast_root", code_snippet = "root", metadata= {"info" : "links to all"}, start_point = (-1,0), end_point = (-1,3))

    def create_node(self, 
                    node_type : str = None, 
                    code_snippet : str = None,
                    metadata : dict = dict(),
                    start_point : tuple[int,int] = (None, None),
                    end_point : tuple[int,int] = (None, None)) -> UASTNode: 
            
        node = UASTNode(id = self.assigned_id, node_type = node_type, code_snippet = code_snippet, metadata = metadata, start_point = start_point, end_point = end_point, children= list(), parents = list())
        self.add_node(node)
        return node
    
    def add_edge(self, node1 : UASTNode = None, node2 : UASTNode = None, directed_relation : str = None, metadata : dict = dict())-> UASTEdge:
        edge = UASTEdge(start_id = node1.id, end_id = node2.id, directed_relation = directed_relation, metadata = metadata)
        node2.parents.append(node1.id)
        node1.children.append(node2.id)
        self.edges.append(edge)
        return edge
    
    def get_node(self, id : int) -> UASTNode:
        return self.nodes[id]
    
    def get_nodes_of_type(self, node_type : str) -> list[int]:
        return self.nodes_of_type[node_type]
    
    def get_children(self, node : UASTNode) -> list[int]:
        return node.children
    
    def get_parents(self, node : UASTNode) -> int: 
        return node.parents

    def print_graph(self, id):
        if id not in self.nodes:
            return
        visited = set()
        
        def dfs(id, visited):
            visited.add(id)
            print(self.nodes[id])
            for child in self.nodes[id].children:
                if child not in visited:
                    dfs(child, visited)
        
        dfs(id, visited)
        del visited
    

    def save_to_file(self, file_path):
        # convert children list to list for serialization
        copy_nodes = self.nodes.copy()
        for k, v in self.nodes.items():
            v.children = list(v.children)
            v.parents = list(v.parents)
            copy_nodes[k] = v


        data = {
            "nodes": {str(k): v.__dict__ for k, v in self.nodes.items()},
            "edges": [edge.__dict__ for edge in self.edges]
        }

        with open(file_path, 'w') as f:
            json.dump(data, f, indent= 4)

        return
    
    def get_json(self):

        copy_nodes = self.nodes.copy()
        for k, v in self.nodes.items():
            v.children = list(v.children)
            v.parents = list(v.parents)
            copy_nodes[k] = v

        data = {
            "nodes": {str(k): v.__dict__ for k, v in self.nodes.items()},
            "edges": [edge.__dict__ for edge in self.edges]
        }
        
        return data
    
    def load_from_json_string(self, obj: str):
        data = json.loads(obj)
        self.nodes = {int(k): UASTNode(**v) for k, v in data["nodes"].items()}
        self.edges = [UASTEdge(**edge) for edge in data["edges"]]
        self.assigned_id = max(self.nodes.keys()) + 1
        for node in self.nodes.values():
            node.start_point = tuple(node.start_point)
            node.end_point = tuple(node.end_point)
        return 

    def load_from_file(self, file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
            self.nodes = {int(k): UASTNode(**v) for k, v in data["nodes"].items()}
            self.edges = [UASTEdge(**edge) for edge in data["edges"]]
            self.assigned_id = max(self.nodes.keys()) + 1
            for node in self.nodes.values():
                node.start_point = tuple(node.start_point)
                node.end_point = tuple(node.end_point)
        return 

    def visualize(self):
        edges_viz = []
        labeldict = {}
        for edge in self.edges:
            edges_viz.append([edge.start_id, edge.end_id])
            labeldict[edge.start_id] = self.nodes[edge.start_id].node_type
            labeldict[edge.end_id] = self.nodes[edge.end_id].node_type
        print(labeldict)
        plt.figure(figsize=(10,10))
        plt.rcParams["font.size"] = 20
        G = networkx.Graph() 
        G.add_edges_from(edges_viz) 
        pos = networkx.spring_layout(G)
        networkx.draw_networkx_labels(G, pos, labels= labeldict, font_size= 12, )
        networkx.draw_networkx_nodes(G, pos, nodelist= self.nodes.keys(), node_size= 300)
        networkx.draw_networkx_edges(G, pos, edgelist= edges_viz)
        plt.show() 
        return
