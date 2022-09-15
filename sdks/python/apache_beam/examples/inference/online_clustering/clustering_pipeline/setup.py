"""Setup.py module for the workflow's worker utilities.
All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.
This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution. Please see the dataflow
boilerplate examples for more detailed setup files that support custom commands
and extra pip installs
"""

import setuptools
from setuptools import find_packages

REQUIREMENTS = [
    "apache-beam[gcp]==2.40.0",
    "transformers==4.21.1",
    "torch==1.12.1",
    "scikit-learn==1.0.2",
]

setuptools.setup(
    name="catalog-dataflow-pipeline",
    version="1.1.1",
    install_requires=REQUIREMENTS,
    packages=find_packages(),
    author="Shubham Krishna",
    author_email="shubham.krishna@ml6.eu",
    py_modules=["config"],
)
