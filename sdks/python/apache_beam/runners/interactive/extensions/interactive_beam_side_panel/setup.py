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

"""
Setup Module to setup Python Handlers for the interactive_beam_side_panel extension.
"""
import os

import setuptools

from jupyter_packaging import combine_commands
from jupyter_packaging import create_cmdclass
from jupyter_packaging import ensure_python
from jupyter_packaging import ensure_targets
from jupyter_packaging import get_version
from jupyter_packaging import install_npm

HERE = os.path.abspath(os.path.dirname(__file__))

# The name of the project
name = "interactive_beam_side_panel"

# Ensure a valid python version
ensure_python(">=3.5")

# Get our version
version = get_version(os.path.join(name, "_version.py"))

lab_path = os.path.join(HERE, name, "labextension")

# Representative files that should exist after a successful build
jstargets = [
    os.path.join(HERE, "lib", "interactivebeamsidepanel.js"),
]

package_data_spec = {name: ["*"]}

data_files_spec = [
    ("share/jupyter/lab/extensions", lab_path, "*.tgz"),
    (
        "etc/jupyter/jupyter_notebook_config.d",
        "jupyter-config",
        "interactive_beam_side_panel.json"),
]

cmdclass = create_cmdclass(
    "jsdeps",
    package_data_spec=package_data_spec,
    data_files_spec=data_files_spec)

cmdclass["jsdeps"] = combine_commands(
    install_npm(HERE, build_cmd="build:all", npm=["jlpm"]),
    ensure_targets(jstargets),
)

with open("README.md", "r") as fh:
  long_description = fh.read()

setup_args = dict(
    name=name,
    version=version,
    url="https://github.com/apache/beam",
    author="ningk",
    description=
    "A side panel providing information of current interactive environment, data sets associated with PCollections, pipeline state, job status and source capture.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    cmdclass=cmdclass,
    packages=setuptools.find_packages(),
    install_requires=[
        "jupyterlab~=2.0",
    ],
    zip_safe=False,
    include_package_data=True,
    license="BSD-3-Clause",
    platforms="Linux, Mac OS X, Windows",
    keywords=["Jupyter", "JupyterLab"],
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Framework :: Jupyter",
    ],
)

if __name__ == "__main__":
  setuptools.setup(**setup_args)
