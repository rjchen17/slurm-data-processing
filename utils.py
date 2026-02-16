from classes import Node
import subprocess

def get_node_info() -> list[Node]:
    """
    Get information for all nodes on the current slurm.

    :return: A list of Nodes
    """
    nodes = [] 
    sinfo_output = subprocess.run(["sinfo", "-N"], capture_output=True)
    node_list = sinfo_output.stdout.decode('utf-8').split("\n")
    # Remove header
    node_list.pop(0) 

    for line in node_list:
        if line.strip() == "":
            continue
        line_parts = line.split()
        node = Node(name=line_parts[0], partition=line_parts[2])
        nodes.append(node)
    
    return nodes
