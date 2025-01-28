"""
To build a flowchart on this project

"""
import matplotlib.pyplot as plt
import networkx as nx

G = nx.DiGraph()
G.add_edges_from([
    ("Data Extraction", "Processing JSON files"),
    ("Processing JSON files", "Data Cleaning"),
    ("Data Cleaning", "Categorize Images and Posts"),
])

pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=3000, font_size=8, font_weight="bold")
plt.show()