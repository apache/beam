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

# apache-beam-jupyterlab-sidepanel

Includes two different side panels:
* The Inspector side panel provides information and controls to run Apache Beam notebooks interactively.
* The Clusters side panel displays all Dataproc clusters managed by Interactive Beam and provides controls to configure cluster usage.

## Requirements

| JupyterLab version | Extension version |
| ------------------ | ----------------- |
| v4                 | >=v4.0.0          |
| v3                 | v2.0.0-v3.0.0     |
| v2                 | v1.0.0            |

## Installation

There are two ways to install the extension:

### 1. Via pip (recommended)

The extension is now available as a Python package on PyPI. You can install it with:

```bash
pip install apache-beam-jupyterlab-sidepanel
```

After installation, rebuild JupyterLab to activate the extension:

```bash
jupyter lab clean
jupyter lab build
```

Then restart JupyterLab. The side panels will be available automatically.


### 2. Via JupyterLab Extension Manager (legacy, will be deprecated soon)

```bash
jupyter labextension install apache-beam-jupyterlab-sidepanel
```

This installs the extension using JupyterLab's legacy extension system.

---

## Notes

- Pip installation is now the preferred method as it handles Python packaging and JupyterLab extension registration seamlessly.
- After any upgrade or reinstallation, always rebuild JupyterLab to ensure the extension is activated.
- For detailed usage and development, refer to the source code and issues on [GitHub](https://github.com/apache/beam).

---

## Contributing

> **Note**: When creating a Python package, the **module name (i.e., the folder name and import path)** must use underscores (`_`) instead of dashes (`-`). Dashes are **not allowed** in Python identifiers.
> However, the **distribution name** (the name used in `pip install ...`) **can** include dashes, and this is a common convention in the Python ecosystem.
> For example:
> - `pip install apache-beam-jupyterlab-sidepanel` ✅
> - `import apache_beam_jupyterlab_sidepanel` ✅
> - `import apache-beam-jupyterlab-sidepanel` ❌ (invalid syntax)

### Install

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below.

```bash
# Clone the repo to your local environment
# Move to apache-beam-jupyterlab-sidepanel directory

# Install dependencies
jlpm
# Build Typescript source
jlpm build
# Link your development version of the extension with JupyterLab
jupyter labextension link .

# Rebuild Typescript source after making changes
jlpm build
# Rebuild JupyterLab after making any changes
jupyter lab build
```

You can watch the source directory and run JupyterLab in watch mode to watch for changes in the extension's source and automatically rebuild the extension and application.

```bash
# Watch the source directory in another terminal tab
jlpm watch
# Run jupyterlab in watch mode in one terminal tab
jupyter lab --watch
```

Now every change will be built locally and bundled into JupyterLab. Be sure to refresh your browser page after saving file changes to reload the extension (note: you'll need to wait for webpack to finish, which can take 10s+ at times).

### Test

To run all tests, under `apache-beam-jupyterlab-sidepanel` directory, simply do:

```bash
# Make sure all dependencies are installed.
jlpm

# Run all tests.
jlpm jest
```

This project uses `ts-jest` to test all ts/tsx files under `src/__tests__` directory.

To run a single test, find out the name of a test in the source code that looks like:

```javascript
it('does ABC', () => {...})
```

Then run:

```bash
jlpm jest -t 'does ABC'
```

### Format and lint

The project uses prettier for formatting and eslint for lint.
Prettier is configured as a plugin used by eslint.
There are pre-configured yarn scripts to execute them.

```bash
# Under apache-beam-jupyterlab-sidepanel directory.

# Make sure dependencies are installed.
# Prettier and eslint are both installed as dev dependencies.
jlpm

# Check format and lint issues.
jlpm eslint:check

# Check then fix in place format and lint issues.
jlpm eslint
```

### Packaging

This JupyterLab extension is distributed as a prebuilt Python package, and includes all necessary frontend assets (TypeScript-compiled JavaScript, styles, and metadata).

#### Build from source

To build the extension locally and package it into a wheel:

```bash
# Ensure build dependencies are available
pip install build jupyterlab jupyter_packaging hatch

# Build the wheel and source tarball
hatch build
```

This will generate `dist/*.whl` and `dist/*.tar.gz` files.

#### Install locally via pip

```bash
pip install .
```

After installation, JupyterLab will automatically discover and enable the extension. You can verify it was installed via:

```bash
jupyter labextension list
```

If properly installed, your extension should appear under the `prebuilt extensions` section.

#### File layout expectations

A typical prebuilt extension should include the following structure under your Python package:

```
apache_beam_jupyterlab_sidepanel/
├── __init__.py
├── _version.py
└── labextension/
    ├── package.json
    ├── install.json
    └── static/
        └── ...
```

These will be copied to:

```
$PREFIX/share/jupyter/labextensions/apache-beam-jupyterlab-sidepanel/
```

### Uninstall

```bash
jupyter labextension uninstall apache-beam-jupyterlab-sidepanel
```
or
```bash
pip uninstall apache-beam-jupyterlab-sidepanel
```
