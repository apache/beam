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

# pytype: skip-file

# It is recommended to import setuptools prior to importing distutils to avoid
# using legacy behavior from distutils.
# https://setuptools.readthedocs.io/en/latest/history.html#v48-0-0
from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py
import subprocess

class CustomBuild(_build_py):
        def run(self):
        # harmless demo command; ok to delete entirely
        subprocess.check_call(["echo", "Custom command worked!"])
        super().run()

setup(
    name="juliaset",
    version="0.0.1",
    description="Julia set workflow package.",
    packages=find_packages(),
    install_requires=[],          # keep empty for staging (--no-isolation)
    cmdclass={"build_py": CustomBuild},  # or drop cmdclass if not needed
)
