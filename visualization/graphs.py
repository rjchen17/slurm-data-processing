import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import numpy as np
import pandas as pd


def cpu_histogram(data: pd.DataFrame) -> Figure:
    """Generate a histogram for CPU hour usage."""
    fig, ax = plt.subplots()
    ax.set_xlabel("CPU Hours")
    ax.set_ylabel("Frequency")
    CPU_hours = data["CPUTimeRAW"].dt.total_seconds() / 3600
    CPU_hours = CPU_hours[CPU_hours > 1]
    bins = 100

    log_min = 0
    log_max = np.log10(max(CPU_hours))
    log_bins = np.logspace(log_min, log_max, bins + 1)
    ax.hist(CPU_hours, log=True, bins=log_bins)
    ax.set_xscale("log")

    return fig


def calendar_histogram(data: pd.DataFrame, interval: str = "week") -> Figure:
    """Generate a histogram that shows job submissions throughout the calendar year."""
    fig, ax = plt.subplots()
    ax.set_xlabel("Job Submissions")
    ax.set_ylabel("Frequency")
