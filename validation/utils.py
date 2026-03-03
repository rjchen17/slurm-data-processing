"""A series of data validation functions that return False if the data has a specific issue, and 
True otherwise (i.e. the data is valid). """
import numpy as np
import pandas as pd

def get_duplicate_jobs(data: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe that contains all jobs that share submit time, account, and CPU usage time. """ 
    submit_counts = data.groupby("Submit").transform("size")
    account_counts = data.groupby("Account").transform("size")
    cpu_counts = data.groupby("ResvCPURAW").transform("size")
    duplicates = data[(submit_counts >= 2) & (account_counts >= 2) & (cpu_counts >= 2)]
    return duplicates.sort_values(by=["Submit", "Account", "ResvCPURAW"])

def get_time_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Get jobs that were run at the same time.  """
    counts = data.groupby("Submit").transform("size")
    duplicates = data[counts >= 2].sort_values(by="Submit")
    return duplicates

def get_job_scores(data: pd.DataFrame, values: str | list = "default") -> pd.DataFrame:
    """Return a list of """
    return
