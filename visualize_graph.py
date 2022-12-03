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
# if len(argv) != 3:
#     print("Not enough arguments.")
checked_node_id_ip = argv[1] 
checked_node_id_port = argv[2] 
print(checked_node_id_ip)
print(checked_node_id_port)
while tuple([checked_node_id_ip, checked_node_id_port]) not in seen:
    resp = requests.get(f"http://{checked_node_id_ip}:{checked_node_id_port}").json()
    print(resp)
    nodes_list.append(tuple(resp["Node"]))
    seen.add(tuple(resp["Node"]))
    checked_node_id_ip, checked_node_id_port = resp["N"][0], resp["N"][1]
nodes_list.append(tuple([checked_node_id_ip, checked_node_id_port]))

print(nodes_list)
for i in range(len(nodes_list)-1):
    G.addEdge(nodes_list[i], nodes_list[i+1])
G.addEdge(nodes_list[i], nodes_list[i+1])
# print(nodes_list)
G.visualize()