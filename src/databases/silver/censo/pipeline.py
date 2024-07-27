"""
This module contains the main function for the pipeline module.

The main function calls the add_hex_dompp.main() function to perform some operation.
"""

from src.databases.silver.censo.steps import add_hex_dompp


def main():
    """
    This is the main function of the pipeline module.
    It calls the add_hex_dompp.main() function to perform some operation.
    """
    add_hex_dompp.main()
