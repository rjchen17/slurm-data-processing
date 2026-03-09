from classes import Node
from pathlib import Path
import subprocess
import os
import re

def get_node_info(slurm_version) -> list[Node]:
    """
    Get information for all nodes on the current slurm.

    Returns:
         A list of Nodes
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

def get_missing_nodes(nodes: list[str], path: Path) -> dict:
    """
    Given information about nodes that are otherwise mising from sinfo.
    
    Args:
        nodes: A list of node names to find info on.
        path: A path to a table of nodes that contains information about node partitions and cpu counts.
    
    Returns:
        A dictionary mapping from nodes: info, in the same format as `write_node_info.py`.
    """
    node_dict = {node: {"name": node} for node in nodes}
    # Gaps in columns between node name and partition info and cpu count
    partition_gap = 3
    cpu_count_gap = 7
    html_re = r'>(.*?)<' # Search for information inside html
    to_remove = ""
    find_info = False
    with open(path, 'r', encoding='utf-8') as node_info:
        lines = node_info.readlines()
        for line in lines:
            for node in nodes:
                if node in line:
                    to_remove = node
                    counter = 0
                    find_info = True
            if find_info:
                if counter == partition_gap:
                    node_dict[to_remove]["partition"] = re.search(html_re, line).group(1)
                if counter == cpu_count_gap:
                    node_dict[to_remove]["cpus"] = int(re.search(html_re, line).group(1))
                    find_info = False
                    nodes.remove(to_remove)
                    to_remove = ""
                counter += 1

    return node_dict
