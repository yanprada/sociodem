"""
Module to setup project
"""

from setuptools import find_packages, setup

setup(
    name="src",
    packages=find_packages(where="src"),
    package_dir={"": "src"},  # <-- ESSA LINHA É ESSENCIAL
    version="0.1.0",
    description="Projeto que visa estimar dados socioeconômicos com fontes de dados públicas.",
    author="Yan Prada",
    license="MIT",
)
