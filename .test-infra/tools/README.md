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

> **PLEASE update this file if you add a new tool or update an existing one**

## Tools

- [Python installer](#python-installer)

### Python installer

It is a tool that installs pyenv and its dependencies, as well as different
versions of python through pyenv. This tool installs pyenv and the different
versions of python only for the user running the script.

#### Use

To use the tool, they only have to download the script and execute it with the
following command.

```bash
bash ./python_installer.sh
```

#### Modification

If you want to modify the python versions to install, you must add/remove the
versions within that are defined within the `python_versions_arr` variable.
Example:

- Original

```bash
python_versions_arr=("3.8.16" "3.9.16" "3.10.10" "3.11.4")
```

- Change

```bash
python_versions_arr=("3.8.10" "3.9.0" "3.10.2")
```
