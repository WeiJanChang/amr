"""
To build a flowchart on this project
ref: https://networkx.org/documentation/stable/reference/generated/networkx.drawing.nx_agraph.pygraphviz_layout.html
"""
import matplotlib.pyplot as plt
import networkx as nx

G = nx.DiGraph()  # create an empty graph with no nodes and no edges
G.add_edges_from([
    ("Data Extraction", "Processing JSON files"),
    ("Processing JSON files", "Data Cleaning"),
    ("Data Cleaning", "Categorize Images and Posts"),
])
# To fix the graph and title overlapping, increase the vertical spacing between nodes (ranksep) and figsize to five the plot more spacce
graph_attrs = {
    'ranksep': '1.5',  # default is 0.25~0.5
    'nodesep': '0.5'
}

pos = nx.nx_agraph.graphviz_layout(G, prog="dot", args='-Granksep=1.5')  # Try using Graphviz layout for top-down flow
plt.figure(figsize=(8,
                    8))  # creates a new figure with a width and height of 8 inches each. It controls how large the entire plot appears on screen or in the saved image.
plt.title("Flowchart", fontsize=16, weight="bold")

nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=3000, font_size=8, font_weight="bold")

plt.savefig(fname='flowchart.pdf', format='pdf')
plt.show()
