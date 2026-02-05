import os
import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    #dates = datetime.data.strptime(date.split(0), "%Y-%M-%D")
    parquet_file = pq.read_table(os.environ["SLURM_DATA_PATH"])

    data = parquet_file.to_pandas()
    print(f"Original table length: {len(date)}")
    date["Reserved"] = date["Reserved"].apply(lambda x: datetime.date.strptime(x.split(0), "%Y-%M-%D"))

    min_date = datetime.date(year=2025, month=1, day=1)
    max_date = datetime.date(year=2025, month=12, day=31)
    date_filtered = data[(data["Reserved"] < min_date) | (data["Reserved"] > max_date)]
    date_filtered_jobs = len(date_filtered)
    print(f"Table with date filters length: {date_filtered_jobs}") 

    MPI = date_filtered[date_filtered["AllocNodes"] >= 2]
    MPI_jobs = len(MPI)
    print(f"{MPI_jobs} jobs using MPI, corresponding to an MPI usage of {MPI_jobs / date_filtered_jobs}")
    
