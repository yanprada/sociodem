"""
This module contains the ANEEL gold pipeline.

The pipeline is executed by calling the main() function, which in turn 
calls the process_aneel_gold.main() function to start the pipeline.
"""

from src.databases.gold.aneel.steps import process_aneel_gold


def main():
    """
    This is the main function that executes the ANEEL gold pipeline.
    It calls the process_aneel_gold.main() function to start the pipeline.
    """
    process_aneel_gold.main()
