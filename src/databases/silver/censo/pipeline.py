"""
This module contains the main function for the pipeline module.

The main function calls the add_hex_dompp.main() function to perform some operation.
"""

from src.databases.silver.censo.steps import a_dompp_add_hex, b_dompp_group_by_hex


def main():
    """
    This is the main function of the pipeline module.
    It calls the add_hex_dompp.main() function to perform some operation.
    """
    a_dompp_add_hex.main()
    b_dompp_group_by_hex.main()
