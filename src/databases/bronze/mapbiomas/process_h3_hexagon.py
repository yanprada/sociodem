"""
This module contains functions to process H3 hexagon data.

The main function, `main()`, retrieves the folder path from the CONTRACT dictionary,
counts the number of files in the folder, and then loads the data for each partition.

The `load_data()` function loads data from a specific partition and returns a pandas DataFrame
grouped by "hex_col", "value", and "size".

"""

from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from tqdm import tqdm
import h3

from src.tools.utils.constants import HEX_RESOLUTION
from src.tools.utils.read import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts
from src.tools.utils.common import count_files

CONTRACT = get_mapbiomas_contracts("bronze")


@save_parquet_decorator("bronze", CONTRACT["mapbiomas_2022"], save_pq=False)
def load_data(partition: int, folder_path: str):
    """
    Load data from a specific partition.

    Args:
        partition (int): The partition number.
        folder_path (str): The path to the folder containing the data.

    Returns:
        pandas.DataFrame: The loaded data grouped by "hex_col", "value", and "size".
    """
    path = os.path.join(folder_path, f"mapbiomas_2022_{partition}.parquet")
    reader = Reader(CONTRACT["mapbiomas_2022"])
    df = reader.read_parquet(path)
    df["hex_col"] = df.apply(lambda x: h3.geo_to_h3(x.lat, x.lng, HEX_RESOLUTION), 1)
    return df.groupby(["hex_col", "value"], as_index=False).size()


def process_partition(partition: int, folder_path: str):
    """
    Process a partition of data.

    Args:
        partition (int): The partition number.
        folder_path (str): The path to the folder containing the data.

    Returns:
        The processed data.

    """
    return load_data(partition, folder_path)


def main():
    """
    This is the main function that processes the H3 hexagon data.

    It retrieves the folder path from the CONTRACT dictionary,
    counts the number of files in the folder,
    and then loads the data for each partition in parallel using ProcessPoolExecutor.
    """
    folder_path = CONTRACT["mapbiomas_2022"]["physicalPath"]
    num_files = count_files(folder_path)

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_partition, partition, folder_path)
            for partition in range(num_files)
        ]
        for future in tqdm(
            as_completed(futures), total=num_files, desc="Processing H3 hexagon data"
        ):
            _ = future.result()  # Process the result if needed


if __name__ == "__main__":
    main()
