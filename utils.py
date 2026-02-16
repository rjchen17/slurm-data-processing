from classes import Node
import subprocess
import os

def get_node_info(slurm_version) -> list[Node]:
    """
    Get information for all nodes on the current slurm.

    :return: A list of Nodes
    """
    nodes = [] 
    sinfo_output = subprocess.run(f"module purge && module load {slurm_version} && sinfo -N", capture_output=True, shell=True, executable="/bin/bash")
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
