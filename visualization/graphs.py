"""Generate matplotlib graphs and figures."""

import logging

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import numpy as np
import pandas as pd
from numpy.testing.print_coercion_tables import print_new_cast_table

logger = logging.getLogger(__name__)
logging.basicConfig(level="INFO")


def align_axes(axes_list, align_x=True, align_y=True):
    """Aligns the x and/or y axis limits for a list of matplotlib Axes objects."""

    # Initialize limits with current values from the first axis
    x_min, x_max = axes_list[0].get_xlim()
    y_min, y_max = axes_list[0].get_ylim()

    # Find the global min/max across all provided axes
    for ax in axes_list:
        curr_x_min, curr_x_max = ax.get_xlim()
        curr_y_min, curr_y_max = ax.get_ylim()

        x_min = min(x_min, curr_x_min)
        x_max = max(x_max, curr_x_max)
        y_min = min(y_min, curr_y_min)
        y_max = max(y_max, curr_y_max)

    # Apply the global limits back to all axes
    for ax in axes_list:
        if align_x:
            ax.set_xlim(x_min, x_max)
        if align_y:
            ax.set_ylim(y_min, y_max)


def cpu_hour_histogram(data: pd.DataFrame, bins=100) -> tuple[Figure, Axes]:
    """Generate a histogram of CPU hour usage."""
    fig, ax = plt.subplots()
    ax.set_xlabel("CPU Hours")
    ax.set_ylabel("Frequency")
    CPU_hours = data["CPUTimeRAW"].dt.total_seconds() / 3600
    CPU_hours = CPU_hours[CPU_hours > 1]

    log_min = 0
    log_max = np.log10(max(CPU_hours))
    log_bins = np.logspace(log_min, log_max, bins + 1)
    ax.hist(CPU_hours, log=True, bins=log_bins)
    ax.set_xscale("log")

    return fig, ax


def cpu_core_histogram(data: pd.DataFrame, bin_width=2) -> tuple[Figure, Axes]:
    """
    Generate a histogram of requested CPU cores.

    Args:
        data: A table of sacct data.
        bin_width: The amount of CPU counts to be included in each bar of the histogram.
    """
    fig, ax = plt.subplots()
    ax.set_xlabel(f"Requested Cores (bars of width {bin_width})")
    ax.set_ylabel("Frequency")
    requested_cores = data["ReqCPUS"]
    ax.hist(requested_cores, log=True, bins=range(1, requested_cores.max(), bin_width))

    return fig, ax


def cpu_proportion_histogram(
    data: pd.DataFrame, nodes: dict, bins=10
) -> tuple[Figure, Axes]:
    """For single node jobs, generate a histogram of the proportion of the total CPUs of the node requested."""

    fig, ax = plt.subplots()
    ax.set_xlabel("Proportion of total cores requested")
    ax.set_ylabel("Frequency")

    if (data["AllocNodes"] > 1).any():
        logger.warning(
            "Multinode jobs passed. Some logging information may be incorrect. "
        )

    requested_cores = data["ReqCPUS"]
    node_cpu_limits = data["NodeList"].map(lambda x: nodes[x]["cpus"])
    node_usage = requested_cores / node_cpu_limits
    ax.hist(node_usage, log=True, bins=bins)

    return fig, ax


def calendar_histogram(data: pd.DataFrame, interval: str = "week") -> Figure:
    """Generate a histogram that shows job submissions throughout the calendar year."""
    fig, ax = plt.subplots()
    ax.set_xlabel("Job Submissions")
    ax.set_ylabel("Frequency")
    raise NotImplementedError
