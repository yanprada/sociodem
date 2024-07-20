"""
This module contains the ANEEL data processing pipeline for silver schema.

The pipeline consists of the following steps:
1. Joins UCBT and PONNOT
2. Fixes after join PONNOT without match

To run the pipeline, call the main() function.
"""

from src.databases.silver.aneel import (
    join_ucbt_and_ponnot,
    fix_after_join_ponnot_without_match,
    process_aneel_after_fixes,
    process_hex_ids_aneel,
)


def main():
    """
    Runs the ANEEL data processing pipeline.

    This function executes the following steps:
    1. Joins UCBT and PONNOT
    2. Fixes after join PONNOT without match
    """
    join_ucbt_and_ponnot.main()
    fix_after_join_ponnot_without_match.main()
    process_aneel_after_fixes.main()
    process_hex_ids_aneel.main()
