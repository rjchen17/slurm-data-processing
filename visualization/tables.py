"""Generate tables in .csv-style formats."""

import pandas as pd


def cpu_usage_table(usage: dict, sort_by="uptime"):
    """
    Create a table of partitions sorted by CPU usage.

    Args:
        usage: A dict mapping from partitions to their usage information. The expected format is
            partition: {data_name: data, data_name: data...}. The number of data points and their
            names is immaterial.

    Returns:

    """

    data = pd.DataFrame.from_dict(usage, orient="index")
    data = data.sort_values(sort_by, ascending=False)

    return data.to_csv()
