"""Get information from a sacct data table in parquet format"""

import os
import sys
import datetime
import json
from argparse import ArgumentParser
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from classes import Node
from utils import get_missing_nodes
from validation.utils import get_time_duplicates, get_duplicates


def parse_args():

    parser = ArgumentParser()

    parser.add_argument(
        "-n",
        "--node_info",
        type=str,
        default="nodes.json",
        help="Path to serialized list of Node objects. ",
    )
    args = parser.parse_args()
    return args


def read_node_info(path: str | Path) -> dict:
    """Get a list of Nodes from file, specified by the 'node_list' argument"""
    with open(path, "r", encoding="utf-8") as node_file:
        node_dict = json.load(node_file)
    return node_dict


def run_duplicate_analysis(data: pd.DataFrame) -> None:

    duplicates = get_duplicates(data)
    print(f"{len(duplicates) / len(data)} proportion of data are dupliactes. ")


def get_cpu_histogram(data: pd.DataFrame, dest: str | Path = "hist.png") -> None:
    """Generate a histogram for CPU hour usage."""
    plt.xlabel("CPU Hours")
    plt.ylabel("Frequency")
    CPU_hours = data["CPUTimeRAW"].dt.total_seconds() / 3600
    CPU_hours = CPU_hours[CPU_hours > 1]
    bins = 100

    log_min = 0
    log_max = np.log10(max(CPU_hours))
    log_bins = np.logspace(log_min, log_max, bins + 1)
    plt.hist(CPU_hours, log=True, bins=log_bins)
    plt.xscale("log")
    plt.savefig(dest)
    return


def cluster_capability_analysis(nodes: dict) -> int:
    """Given a dict of node information, compute the total possible CPU hours on the cluster."""

    cpus = sum([int(nodes[node]["cpus"]) for node in nodes])
    return cpus * 365 * 24


def main(args):

    pd.set_option("display.max_columns", None)
    parquet_file = pq.read_table(os.environ["SLURM_DATA_PATH"])

    data = parquet_file.to_pandas()
    get_cpu_histogram(data)

    print(f"Original table length: {len(data)}")
    # Start has a dtype of datetime64, numpy's version of datetime
    min_date = datetime.datetime(year=2025, month=1, day=1)
    max_date = datetime.datetime(year=2025, month=12, day=31)
    # AFAIK comparison works fine between the two
    date_filtered = data[(data["Start"] > min_date) | (data["Start"] < max_date)]
    date_filtered_jobs = len(date_filtered)
    print(f"Table with date filters length: {date_filtered_jobs}")

    run_duplicate_analysis(data)

    # Mask that checks for multiple nodes
    multi_node_mask = date_filtered["AllocNodes"] >= 2
    # Tilde notation acts as as a "not"
    tier_3 = date_filtered[multi_node_mask]  # tier_3 = jobs ran on 2 or more nodes
    tier_1_and_2 = date_filtered[~multi_node_mask]
    node_dict = read_node_info(path=args.node_info)

    # Some node names aren't coming up in the current version of slurm. We can search for them via
    # the `get_missing_nodes` function. If they are still missing, we will discard them.
    renamed_nodes_mask = ~tier_1_and_2["NodeList"].isin(node_dict.keys())
    renamed_nodes_data = tier_1_and_2[renamed_nodes_mask]
    unique_nodes = renamed_nodes_data["NodeList"].dropna().unique().tolist()
    missing_nodes = get_missing_nodes(unique_nodes, "nodes.txt")

    found_nodes = {}  # Nodes whose info was retrievable
    for node, node_info in missing_nodes.items():
        if "cpus" in node_info:
            found_nodes[node] = node_info
    all_nodes = node_dict | found_nodes
    total_possible_hours = cluster_capability_analysis(all_nodes)
    print(f"{total_possible_hours:,}")
    valid_nodes_mask = tier_1_and_2["NodeList"].isin(all_nodes.keys())
    valid_tier_1_and_2 = tier_1_and_2[valid_nodes_mask]

    # Use CPU limits based on scontrol to differentiate tier 1 and 2 jobs
    node_cpu_limits = valid_tier_1_and_2["NodeList"].map(lambda x: all_nodes[x]["cpus"])
    cpu_allocation_mask = valid_tier_1_and_2["AllocCPUS"] >= node_cpu_limits
    tier_1 = valid_tier_1_and_2[~cpu_allocation_mask]
    tier_2 = valid_tier_1_and_2[cpu_allocation_mask]

    date_filtered["CPUTimeRAW"] = date_filtered["CPUTimeRAW"].dt.total_seconds() / 3600
    total_cpu = date_filtered["CPUTimeRAW"].sum()
    for index, dataframe in enumerate([tier_1, tier_2, tier_3]):
        get_cpu_histogram(data=dataframe, dest=str(index + 1) + "_hist.png")
        num_jobs = len(dataframe)
        tier_cpu = dataframe["CPUTimeRAW"].dt.total_seconds() / 3600
        total_tier_cpu = tier_cpu.sum()
        print(f">>> Tier {index + 1} >>>")
        print(f"{num_jobs} jobs ran. Proportion: {num_jobs / date_filtered_jobs}. ")
        print(
            f"Total CPU hours: {total_tier_cpu:,.2f}. Proportion: {total_tier_cpu / total_cpu:.2f}. "
        )
        tier_mean = np.mean(tier_cpu)
        print(
            f"Descriptive statistics: mean of {tier_mean:,.2f} \u00b1{np.std(tier_cpu):.4f}, "
            f"median of {np.nanmedian(tier_cpu):,.2f}. "
        )


if __name__ == "__main__":
    args = parse_args()
    main(args)
