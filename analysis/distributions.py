"""Get various distributions of cluster statistics."""

import logging

import pandas as pd
from scipy.stats import pearsonr, spearmanr

logger = logging.getLogger(__name__)
logging.basicConfig(level="INFO")


def get_wait_correlations(
    data: pd.DataFrame, partitions=["standard", "gpu", "interactive", "preempt"]
) -> dict:
    """
    Get correlations between certain sacct columns and wait times. The function also checks for partitions that had
    GPU requests and calculates additional correlations for those partitions.

    Args:
        data: A sacct data table.
        partitions: A list of partitions to analyze. Empty partitions will be noted in a logger warning.

    Returns:
        A dictionary that maps from each partition to the relevant correlation. E.g.
    wait_statistics["standard"]["ReqNodes"]["pearsonr"] signifies Pearson's R between nodes requested on the
    standard partition and wait time.
    """

    # Keep only the partitions that exist in the cluster
    cluster_partitions = set(data["Partitions"].unique())
    partitions = [p for p in partitions if p in cluster_partitions]
    empty_partitions = set(partitions) & cluster_partitions
    if empty_partitions != set():
        logger.warning(
            f"The following partitions are not in the data provided: {empty_partitions}. "
        )

    # Check which partitions use GPU
    gpu_partitions = [
        partition
        for partition in partitions
        if data[data["Partition"] == partition]["ReqGRES"].dropna() is not None
    ]

    wait_statistics = {}

    base_cols = ["ReqNodes", "ReqCPUS", "Timelimit"]
    for partition in partitions:

        if partition in gpu_partitions:
            cols_to_analyze = base_cols + ["gpus"]
        else:
            cols_to_analyze = base_cols

        partition_statistics = {}
        wait_time = data[data["Partition"] == partition]["Reserved"]

        partition_statistics["pearsonr"] = {
            column: pearsonr(wait_time, data[data["Partition"] == partition][column])
            for column in cols_to_analyze
        }
        partition_statistics["spearmanr"] = {
            column: spearmanr(wait_time, data[data["Partition"] == partition][column])
            for column in cols_to_analyze
        }
        wait_statistics[partition] = partition_statistics

    return wait_statistics


def get_submission_frequency(
    data: pd.DataFrame, frequency: pd.Timedelta, min_date=None, max_date=None
):

    if min_date is None:
        min_date = data["Submit"].min()
    if max_date is None:
        max_date = data["Submit"].max()
    date_range = max_date - min_date
    return len(data) / (date_range / frequency)


def wait_calendar_heatmap():

    return
