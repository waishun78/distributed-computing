# First networkx library is imported
# along with matplotlib
import networkx as nx
import matplotlib.pyplot as plt
import requests


# Defining a Class
class GraphVisualization:

    def __init__(self):
        # visual is a list which stores all
        # the set of edges that constitutes a
        # graph
        self.visual = []

    # addEdge function inputs the vertices of an
    # edge and appends it to the visual list
    def addEdge(self, a, b):
        temp = [a, b]
        self.visual.append(temp)

    # In visualize function G is an object of
    # class Graph given by networkx G.add_edges_from(visual)
    # creates a graph with a given list
    # nx.draw_networkx(G) - plots the graph
    # plt.show() - displays the graph
    def visualize(self):
        G = nx.DiGraph()
        G.add_edges_from(self.visual)
        nx.draw_networkx(G)
        plt.show()


# Driver code
G = GraphVisualization()
seen = set()
nodes_list = []
from sys import argv
checked_node_id = argv[1] if len(argv) == 2 else 6000
while checked_node_id not in seen:
    resp = requests.get(f"http://0.0.0.0:{checked_node_id}").json()
    nodes_list.append(resp["Node"])
    seen.add(resp["Node"])
    checked_node_id = resp["N"]
nodes_list.append(checked_node_id)

for i in range(len(nodes_list)-1):
    G.addEdge(nodes_list[i], nodes_list[i+1])
G.addEdge(nodes_list[i], nodes_list[i+1])
# print(nodes_list)
G.visualize()