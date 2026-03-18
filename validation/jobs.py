"""Job-based validation functions."""

import numpy as np
import pandas as pd


def get_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Get jobs that were submitted at the same time, by the same person, and ran for the same
    amount of time. Should be a decent proxy for dupliactes, for now."""

    # Apply filter. This should remove any jobs that ran for 0 seconds, which we don't necessarily
    # want to treat as "dupliactes"
    data = data[
        (data["ResvCPURAW"] > 0.0) & (~data["State"].isin(["CANCELLED", "TIMEOUT"]))
    ].copy()
    # Sort by fields. Choice of indexing column (e.g. ["Submit"] is immaterial
    group_counts = data.groupby(["Submit", "Account", "Elapsed"])["Submit"].transform(
        "size"
    )
    duplicates = data[group_counts >= 2]
    return duplicates.sort_values(by=["Submit", "Account", "Elapsed"])


def get_time_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Get jobs that were run at the same time."""
    counts = data.groupby("Submit").transform("size")
    duplicates = data[counts >= 2].sort_values(by="Submit")
    return duplicates


def get_job_scores(data: pd.DataFrame, values: str | list = "default") -> pd.DataFrame:
    """A function that assigns scores to jobs based on their chance of being duplicates."""
    raise NotImplementedError
