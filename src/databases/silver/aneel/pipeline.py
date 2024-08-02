"""
This module contains the ANEEL data processing pipeline for silver schema.

The pipeline consists of the following steps:
1. Joins UCBT and PONNOT
2. Fixes after join PONNOT without match

To run the pipeline, call the main() function.
"""

from src.databases.silver.aneel.steps import (
    a_join_ucbt_and_ponnot,
    b_fix_after_join_ponnot_without_match,
    c_process_aneel_after_fixes,
    d_process_hex_ids_aneel,
    e_group_by_hex,
)


def main():
    """
    Runs the ANEEL data processing pipeline.

    This function executes the following steps:
    1. Joins UCBT and PONNOT
    2. Fixes after join PONNOT without match
    """
    a_join_ucbt_and_ponnot.main()
    b_fix_after_join_ponnot_without_match.main()
    c_process_aneel_after_fixes.main()
    d_process_hex_ids_aneel.main()
    e_group_by_hex.main()
