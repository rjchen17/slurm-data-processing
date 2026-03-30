"""Get information from a sacct data table in parquet format. Most of the parts of the script are in their own
functions, many of which return None but add information to the logger."""

import os
import sys
import datetime
import json
import logging
from argparse import ArgumentParser
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.distributions import get_submission_frequency
from classes import Node
from utils import get_missing_nodes
from validation.jobs import get_time_duplicates, get_duplicates
from validation.cluster import capability_analysis
from visualization.graphs import (
    cpu_hour_histogram,
    align_axes,
    cpu_core_histogram,
    cpu_proportion_histogram,
)
from visualization.tables import cpu_usage_table

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler("test.log"))
logging.basicConfig(level="INFO")


def parse_args():
    """Parse relevant command line args."""
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
    print(f"{len(duplicates) / len(data)} proportion of data are duplicates. ")


def run_frequency_analysis(data: pd.DataFrame, min_date, max_date) -> None:

    logger.info("Running submission frequency analysis. ")
    frequencies = [
        pd.Timedelta(1, "h"),
        pd.Timedelta(1, "D"),
        pd.Timedelta(7, "D"),
        pd.Timedelta(14, "D"),
        pd.Timedelta(30, "D"),
    ]
    labels = ["1 Hour", "1 Day", "7 Days", "14 Days", "30 Days"]

    for label, frequency in zip(labels, frequencies):
        count = get_submission_frequency(
            data, frequency, min_date=min_date, max_date=max_date
        )
        logger.info(f"{count} jobs were submitted every {label}. ")

    logger.info("Submission frequency analysis complete. ")


def run_wait_analysis(data: pd.DataFrame) -> None:

    logger.info("Running wait time analysis. ")
    outlier_threshold = pd.Timedelta(30, "days")
    dropna_waits = data["Reserved"].dropna()
    mean_wait, std_wait = np.mean(dropna_waits), np.std(dropna_waits)
    logger.info(
        f"Jobs on the cluster waited for a mean of {mean_wait / np.timedelta64(1, 'h')} hours \u00b1{std_wait / np.timedelta64(1, 'h')}. "
    )
    logger.info(
        f"{dropna_waits[dropna_waits > pd.Timedelta(30, "days")].sum()} jobs on the cluster had a wait time of greater than {outlier_threshold}. "
    )
    public_partitions = ["standard", "gpu", "interactive", "preempt"]
    for partition in public_partitions:
        dropna_waits = data[data["Partition"] == partition]["Reserved"].dropna()
        mean_wait, std_wait = np.mean(dropna_waits), np.std(dropna_waits)
        logger.info(
            f"Jobs in the {partition} partition waited for a mean of {mean_wait / np.timedelta64(1, 'h')} hours \u00b1{std_wait / np.timedelta64(1, 'h')}. "
        )

    logger.info("Wait time analysis complete. ")


def run_usage_analysis(data: pd.DataFrame, nodes: dict) -> dict:
    """
    Get the CPU usage per partition.

    Args:
        data: A table of sacct data.
        nodes: Per-node information that includes total CPUs.

    Returns:
        A dict mapping from partition names to possible and actual CPU usage.
    """
    partition_cpus = {}
    usage = {}
    for node_data in nodes.values():
        if node_data["partition"] not in partition_cpus:
            partition_cpus[node_data["partition"]] = node_data["cpus"]
        else:
            partition_cpus[node_data["partition"]] += node_data["cpus"]

    for partition in partition_cpus:
        possible_cpu = partition_cpus[partition] * 365 * 24
        jobs = data[data["Partition"] == partition]
        job_cpu_hours = jobs["CPUTimeRAW"].dt.total_seconds() / 3600
        mean, std = np.mean(job_cpu_hours), np.std(job_cpu_hours)
        used_cpu = int(job_cpu_hours.sum())
        usage[partition] = {
            "used": used_cpu,
            "possible_cpu": possible_cpu,
            "uptime": used_cpu / possible_cpu,
            "mean": mean,
            "std": std,
        }

    return usage


def main(args):

    pd.set_option("display.max_columns", None)
    parquet_file = pq.read_table(os.environ["SLURM_DATA_PATH"])
    data = parquet_file.to_pandas()

    # Start has a dtype of datetime64, numpy's version of datetime
    min_date = datetime.datetime(year=2025, month=1, day=1)
    max_date = datetime.datetime(year=2025, month=12, day=31)
    # AFAIK comparison works fine between the two
    date_filtered = data[(data["Start"] > min_date) | (data["Start"] < max_date)]
    date_filtered_jobs = len(date_filtered)
    print(f"Original table length: {len(data)}")
    print(f"Table with date filters length: {date_filtered_jobs}")

    run_frequency_analysis(date_filtered, min_date=min_date, max_date=max_date)
    run_wait_analysis(date_filtered)
    run_duplicate_analysis(data)

    # Mask that checks for multiple nodes
    multi_node_mask = date_filtered["AllocNodes"] >= 2
    # Tilde notation acts as a "not"
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
    for (
        _,
        node_info,
    ) in (
        all_nodes.items()
    ):  # Asterisk is used to mark "default partition". In future, maybe just remove asterisks?
        if node_info["partition"] == "standard*":
            node_info["partition"] = "standard"

    usage = run_usage_analysis(data, all_nodes)
    with open("partition_usage.csv", "w") as csv_file:
        csv_file.write(cpu_usage_table(usage))

    used_nodes = {
        node: node_info
        for node, node_info in all_nodes.items()
        if usage[node_info["partition"]]["uptime"] > 0.01
    }

    capability_analysis(data=date_filtered, nodes=used_nodes)
    capability_analysis(data=date_filtered, nodes=all_nodes)

    valid_nodes_mask = tier_1_and_2["NodeList"].isin(all_nodes.keys())
    valid_tier_1_and_2 = tier_1_and_2[valid_nodes_mask]

    # Use CPU limits based on scontrol to differentiate tier 1 and 2 jobs
    node_cpu_limits = valid_tier_1_and_2["NodeList"].map(lambda x: all_nodes[x]["cpus"])
    cpu_allocation_mask = valid_tier_1_and_2["AllocCPUS"] >= 0.5 * node_cpu_limits
    tier_1 = valid_tier_1_and_2[~cpu_allocation_mask]
    tier_2 = valid_tier_1_and_2[cpu_allocation_mask]

    fig, _ = cpu_core_histogram(valid_tier_1_and_2)
    fig.show()
    fig2, _ = cpu_proportion_histogram(valid_tier_1_and_2, all_nodes)
    fig2.show()

    # Split tier 1 jobs further
    tier_1a = tier_1[tier_1["AllocCPUS"] == 1]
    tier_1b = tier_1[tier_1["AllocCPUS"] == 2]
    tier_1c = tier_1[tier_1["AllocCPUS"] > 2]
    all_tiers = [tier_1, tier_1a, tier_1b, tier_1c, tier_2, tier_3]
    tier_labels = ["1", "1a", "1b", "1c", "2", "3"]

    date_filtered["CPUTimeRAW"] = date_filtered["CPUTimeRAW"].dt.total_seconds() / 3600
    total_cpu = date_filtered["CPUTimeRAW"].sum()

    figs = []
    # Use zip to iterate through the dataframes and their labels simultaneously
    for dataframe, label in zip(all_tiers, tier_labels):
        figs.append(cpu_hour_histogram(data=dataframe))

        num_jobs = len(dataframe)
        tier_cpu = dataframe["CPUTimeRAW"].dt.total_seconds() / 3600
        total_tier_cpu = tier_cpu.sum()

        print(f"\n>>> Tier {label} >>>")
        print(
            f"{num_jobs:,} jobs ran. Proportion: {num_jobs / date_filtered_jobs:.4f}. "
        )
        print(
            f"Total CPU hours: {total_tier_cpu:,.2f}. Proportion: {total_tier_cpu / total_cpu:.4f}. "
        )

        tier_mean = np.mean(tier_cpu)
        print(
            f"Descriptive statistics: mean of {tier_mean:,.2f} \u00b1{np.std(tier_cpu):.4f}, "
            f"median of {np.nanmedian(tier_cpu):,.2f}. "
        )

    align_axes([fig[1] for fig in figs])
    for fig, tier_label in zip(figs, tier_labels):
        fig[0].savefig(f"tmp/{tier_label}_cpu.png")


if __name__ == "__main__":
    args = parse_args()
    main(args)
