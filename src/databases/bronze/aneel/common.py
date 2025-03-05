"""
This module provides utility functions for handling and processing file sizes
from a specified directory. It includes functions to flatten a list of lists
and to split files into categories based on their sizes.

Functions:
    flat_list(files_dict: DefaultDict[str, List[str]]) -> List[str]:
        Flattens a dictionary of lists into a single list.

    split_file_sizes() -> Tuple[List[str], List[str], List[str]]:
        Splits files into three categories based on their sizes: large, medium, and small.

"""

import os
import glob
from collections import defaultdict
from typing import List, Tuple, DefaultDict
import pandas as pd
from src.databases.bronze.aneel.config import CONTRACT_RAW_ENERGY


def flat_list(files_dict: DefaultDict[str, List[str]]) -> List[str]:
    """
    Flattens a list of lists.

    Args:
        files_dict (dict): A dictionary containing lists of files.

    Returns:
        list: The flattened list.
    """
    temp_files = []
    if not files_dict:
        return temp_files
    for _, files in files_dict.items():
        temp_files.extend(files)
    return temp_files


def split_file_sizes() -> Tuple[List[str], List[str], List[str]]:
    """
    Splits the file sizes into four lists based on their sizes.

    Returns:
        tuple: A tuple containing two lists - large_files and small_files.
               large_files: List of file ids with sizes greater than or equal to 800MB.
               medium_files: List of file ids with sizes greater than or equal
                            to 100MB and less than 800MB.
               small_files: List of file ids with sizes less than 100MB.
    """
    large_files = defaultdict(list)
    medium_files = defaultdict(list)
    small_files = defaultdict(list)
    split_size = 800 * 1024 * 1024
    df_aneel_ids = pd.DataFrame(
        {
            "company_ids": [
                os.path.basename(f)
                for f in glob.glob(
                    os.path.join(
                        CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"], "*.gdb.zip"
                    )
                )
            ]
        }
    )
    for company_id in df_aneel_ids["company_ids"]:
        if " - " not in company_id:
            continue
        year = company_id.split(" - ")[1].split("-")[0]
        file_path = os.path.join(
            CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"],
            company_id,
        )
        file_size = os.path.getsize(file_path)
        if file_size >= split_size:
            large_files[year].append(company_id)
        elif (split_size / 8) <= file_size < split_size:
            medium_files[year].append(company_id)
        else:
            small_files[year].append(company_id)
    large_files = flat_list(large_files)
    medium_files = flat_list(medium_files)
    small_files = flat_list(small_files)
    return large_files, medium_files, small_files
