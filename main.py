"""Get information from a sacct data table in parquet format"""

import os
import datetime
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from argparse import ArgumentParser
from classes import Node

def parse_args():
    
    parser = ArgumentParser()

    parser.add_argument("-n", "--node_info", type=str, default="nodes.json", help="Path to serialized list of Node objects. ")
    args = parser.parse_args()
    return args

def read_node_info() -> dict:
    """Get a list of Nodes from file, specified by the 'node_list' argument"""
    with open(args.node_info, 'r') as node_file:
        node_dict = json.load(node_file)
    return node_dict

def main():

    pd.set_option('display.max_columns', None)
    parquet_file = pq.read_table(os.environ["SLURM_DATA_PATH"])

    data = parquet_file.to_pandas()

    print(f"Original table length: {len(data)}")
    # Start has a dtype of datetime64, numpy's version of datetime
    min_date = datetime.datetime(year=2025, month=1, day=1)
    max_date = datetime.datetime(year=2025, month=12, day=31)
    # AFAIK comparison works fine between the two
    date_filtered = data[(data["Start"] > min_date) | (data["Start"] < max_date)]
    date_filtered_jobs = len(date_filtered)
    print(f"Table with date filters length: {date_filtered_jobs}") 
    
    # Mask that checks for multiple nodes
    multi_node_mask = date_filtered["AllocNodes"] >= 2 
    # Tilde notation acts as as a "not"
    tier_3 = date_filtered[multi_node_mask] # tier_3 = jobs ran on 2 or more nodes
    tier_1_and_2 = date_filtered[~multi_node_mask] 
    node_dict = read_node_info()
    
    # Some node names aren't coming up in the current version of slurm. Assuming that the nodes were 
    # removed/renamed, for now just filter out all those nodes
    renamed_nodes_mask = ~tier_1_and_2["NodeList"].isin(node_dict.keys())
    renamed_nodes_data = tier_1_and_2[renamed_nodes_mask]
    valid_tier_1_and_2 = tier_1_and_2[~renamed_nodes_mask]
    
    # Use CPU limits based on scontrol to differentiate tier 1 and 2 jobs
    node_cpu_limits = valid_tier_1_and_2["NodeList"].map(lambda x: node_dict[x]["cpus"])
    cpu_allocation_mask = valid_tier_1_and_2["AllocCPUS"] >= node_cpu_limits
    tier_1 = valid_tier_1_and_2[~cpu_allocation_mask]
    tier_2 = valid_tier_1_and_2[cpu_allocation_mask]

    for index, dataframe in enumerate([tier_1, tier_2, tier_3]):
        num_jobs = len(dataframe)
        print(f"{num_jobs} Tier {index + 1} jobs ran. Proportion: {num_jobs / date_filtered_jobs}")
if __name__ == "__main__":
    args = parse_args()
    main()
