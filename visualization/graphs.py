"""Generate matplotlib graphs and figures."""

import datetime
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

    if (data["AllocNodes"] > 1).any():
        logger.warning(
            "Multinode jobs passed. Some logging information may be incorrect. "
        )

    fig, ax = plt.subplots()
    ax.set_xlabel("Proportion of total cores requested")
    ax.set_ylabel("Frequency")

    requested_cores = data["ReqCPUS"]
    node_cpu_limits = data["NodeList"].map(lambda x: nodes[x]["cpus"])
    node_usage = requested_cores / node_cpu_limits
    ax.hist(node_usage, log=True, bins=bins)

    return fig, ax


def cpu_hours_by_core(
    data: pd.DataFrame, min_cores: int = None, max_cores: int = None, **kwargs
):
    if (data["AllocNodes"] > 1).any():
        logger.warning(
            "Multinode jobs passed. Some logging information may be incorrect."
        )

    min_cores = min_cores if min_cores is not None else data["ReqCPUS"].min()
    max_cores = max_cores if max_cores is not None else data["ReqCPUS"].max()
    cpu_hours_float = data["CPUTimeRAW"].dt.total_seconds() / 3600
    mask = (data["ReqCPUS"] >= min_cores) & (data["ReqCPUS"] <= max_cores)

    stats = cpu_hours_float[mask].groupby(data.loc[mask, "ReqCPUS"]).sum()
    other_cores = cpu_hours_float[~mask].sum()
    stats[len(stats) + 1] = other_cores

    fig, ax = plt.subplots()
    ax.bar(stats.index, stats.values, **kwargs)

    ax.set_xlabel("Number of cores")
    ax.set_ylabel("Total CPU hours")
    ax.set_title(f"CPU Usage: {min_cores} to {max_cores} Cores")

    return fig, ax


def submit_frequency_histogram(
    data: pd.DataFrame,
    offset: pd.Timedelta | pd.DateOffset | str = "1h",
    fig_ax=None,
    **kwargs,
):
    """Get frequency of job submissions according to the provided offset."""

    submissions = data["Submit"]
    if fig_ax is None:
        fig, ax = plt.subplots()
    else:
        fig, ax = fig_ax

    min_date = datetime.datetime(year=2025, month=1, day=1)
    max_date = datetime.datetime(year=2025, month=12, day=31)
    time_intervals = pd.date_range(start=min_date, end=max_date, freq=offset)
    ax.hist(submissions, bins=time_intervals, density=False, histtype="step", log=False)

    return fig, ax


def submit_frequency_metrics(
    data: pd.DataFrame,
    offset: pd.Timedelta | pd.DateOffset | str = "1h",
    fig_ax=None,
    resample_time: pd.Timedelta = None,
    **kwargs,
):
    """Get frequency of job submissions according to the provided offset."""

    submissions = data.set_index("Submit").sort_index()
    if resample_time is not None:
        submissions = submissions.resample(resample_time).sum()
        print(submissions)

    if fig_ax is None:
        fig, ax = plt.subplots()
    else:
        fig, ax = fig_ax

    min_date = datetime.datetime(year=2025, month=1, day=1)
    max_date = datetime.datetime(year=2025, month=12, day=31)
    time_intervals = pd.date_range(start=min_date, end=max_date, freq=offset)
    ax.plot(submissions, time_intervals)

    return fig, ax


def calendar_histogram(data: pd.DataFrame, interval: str = "week") -> Figure:
    """Generate a histogram that shows job submissions throughout the calendar year."""
    fig, ax = plt.subplots()
    ax.set_xlabel("Job Submissions")
    ax.set_ylabel("Frequency")
    raise NotImplementedError
