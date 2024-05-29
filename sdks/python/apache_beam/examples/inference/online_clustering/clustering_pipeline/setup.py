#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Setup.py module for the workflow's worker utilities.
All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.
This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution.
"""

import setuptools
from setuptools import find_packages

REQUIREMENTS = [
    "apache-beam[gcp]==2.40.0",
    "transformers==4.38.0",
    "torch==1.13.1",
    "scikit-learn==1.0.2",
]

setuptools.setup(
    name="catalog-dataflow-pipeline",
    version="1.1.1",
    install_requires=REQUIREMENTS,
    packages=find_packages(),
    author="Apache Software Foundation",
    author_email="dev@beam.apache.org",
    py_modules=["config"],
)
