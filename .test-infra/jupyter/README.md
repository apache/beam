<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

This directory contains Jupyter notebooks for use in gathering and analyzing
test metrics.

# Jupyter Setup

Instructions for installing on Linux using pip+virtualenv:

```shell
virtualenv --python python3 ~/virtualenvs/jupyter
source ~/virtualenvs/jupyter/bin/activate
pip install jupyter
# Optional packages, for example:
pip install pandas matplotlib requests
cd .test-infra/jupyter
jupyter notebook  # Should open a browser window.
```

# Pull Requests

To minimize file size, diffs, and ease reviews, please clear all cell output
(cell -> all output -> clear) before commiting.
