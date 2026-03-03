"""A series of data validation functions that return False if the data has a specific issue, and 
True otherwise (i.e. the data is valid). """
import numpy as np
import pandas as pd

def get_duplicate_jobs(data: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe that contains all jobs that share a JobID with one or more other jobs""" 
    counts = data.groupby("JobID").transform("size")
    duplicates = data[counts >= 2]
    return duplicates

def get_time_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Get jobs that were run at the same time.  """
    counts = data.groupby("Submit").transform("size")
    duplicates = data[counts >= 2].sort_values(by="Submit")
    return duplicates

def get_job_scores(data: pd.DataFrame, values: str | list = "default") -> pd.DataFrame:
    """Return a list of """
    return
