"""
This script is used to download files from an S3 bucket with 
a given prefix.

"""

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)


from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.transportation.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_1")

TRANSPORT_CONTRACTS = execution_parameters["data_contracts"][0]


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_path = TRANSPORT_CONTRACTS["datalake"]["physicalPath"]
    prefix = "release/2024-07-22.0/theme=transportation/"
    requester = HttpRequesterOvertureMaps(prefix, download_path)
    requester.download_files_omf()
    manager.update_status("finished_step_1")
    manager.update_last_run()
