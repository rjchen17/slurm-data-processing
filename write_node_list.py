"""Get all node information on all slurms. Then, write the nodes to a serialized format (json) such that they can be retrieved. """

import subprocess
import json
import os

from utils import get_node_info

def main():
    slurm_versions = os.environ["SLURM_VERSIONS"].split(",")
    # Get list of node names with sinfo
    node_dict = {}   
    for slurm in slurm_versions:
        subprocess.run(f"module purge && module load {slurm}", shell=True, executable="/bin/bash")
        node_list = get_node_info(slurm) 
        for node in node_list:
            scontrol_output = subprocess.run(f"module purge && module load {slurm} && scontrol show node {node.name}", capture_output=True, shell=True, executable="/bin/bash", check=True)
            scontrol_as_string = scontrol_output.stdout.decode().strip()
            # scontrol returns a series of attributes separated by whitespace. The attributes are further separated 
            # from their values by equal signs. The line below parses that into a dictionary. Sometimes there's additional info
            # at the end of the scontrol output, hence the "if '=' in part" checking
            scontrol_dict = {attribute: value for attribute, value in (part.split('=') for part in scontrol_as_string.split() if 'CPU' in part)}
            node.cpus = int(scontrol_dict["CPUTot"])
        # "Append" keys/values to node_dict
        node_dict.update({node.name: node.__dict__ for node in node_list})

    with open("nodes.json", 'w') as nodes_file:
       json.dump(node_dict, nodes_file, indent=1) 

if __name__ == "__main__":

    main()
