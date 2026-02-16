"""Get all node information on the current slurm. Then, write the nodes to a serialized format (json) such that they can be retrieved. """

import subprocess
import json
from utils import get_node_info

def main():
    # Get list of node names with sinfo
    node_list = get_node_info() 
    for node in node_list:
        scontrol_output = subprocess.run(["scontrol", "show", "node", f"{node.name}"], capture_output=True)
        scontrol_as_string = scontrol_output.stdout.decode().strip()

        # scontrol returns a series of attributes separated by whitespace. The attributes are further separated 
        # from their values by equal signs. The line below parses that into a dictionary. Sometimes there's additional info
        # at the end of the scontrol output, hence the "if '=' in part" checking
        scontrol_dict = {attribute: value for attribute, value in (part.split('=') for part in scontrol_as_string.split() if '=' in part)}
        node.cpus = int(scontrol_dict["CPUTot"])
    with open("nodes.json", 'w') as nodes_file:
       json.dump([node.__dict__ for node in node_list], nodes_file, indent=1) 

if __name__ == "__main__":

    main()
