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

A side panel providing information and controls to run Apache Beam notebooks interactively.


## Requirements

* JupyterLab >= 2.0

## Install (WIP)

```bash
jupyter labextension install apache-beam-jupyterlab-sidepanel
```

## Contributing

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

### Uninstall

```bash
jupyter labextension uninstall apache-beam-jupyterlab-sidepanel
```
