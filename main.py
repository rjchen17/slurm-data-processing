import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

if __name__ == "__main__":

    parquet_file = pq.read_table(os.environ["SLURM_DATA_PATH"])
    data = parquet_file.to_pandas()
    print(data.columns)
    print(data.iloc[0])
