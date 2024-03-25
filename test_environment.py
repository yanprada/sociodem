"""
Module to test environment
"""

import sys

REQUIRED_PYTHON = "python3"


def main():
    """Main function to test enviroments

    Raises:
        ValueError: _description_
        TypeError: _description_
    """
    system_major = sys.version_info.major
    if REQUIRED_PYTHON == "python":
        required_major = 2
    elif REQUIRED_PYTHON == "python3":
        required_major = 3
    else:
        raise ValueError(f"Unrecognized python interpreter: {REQUIRED_PYTHON}")

    if system_major != required_major:
        raise TypeError(
            f"This project requires Python {required_major}. Found: Python {sys.version}"
        )
    print(">>> Development environment passes all tests!")


if __name__ == "__main__":
    main()
