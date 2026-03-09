"""Cluster-based validation functions."""

import logging
import pandas as pd
from matplotlib.rcsetup import validate_int

logger = logging.getLogger(__name__)
logging.basicConfig(encoding="utf-8", level=logging.INFO)


def capability_analysis(data: pd.DataFrame, nodes: dict) -> bool:
    """
    Given a dict of node information, compute the total possible CPU hours on the cluster.

    Args:
        data: A table of sacct data
        nodes: Per-node information that includes total CPUs
    Returns:
        True if validation is passed, False otherwise
    """

    logger.info("Running capability analysis. ")
    total_possible_hours = sum([int(nodes[node]["cpus"]) for node in nodes]) * 365 * 24
    hours_ran = int((data["CPUTimeRAW"].dt.total_seconds() / 3600).sum())

    if hours_ran > total_possible_hours:
        validation_passed = False
    else:
        validation_passed = True
    logger.info(
        f"Validation complete. Cluster ran for {hours_ran:,} hours "
        f"of a maximum possible {total_possible_hours:,}. "
    )

    return validation_passed
