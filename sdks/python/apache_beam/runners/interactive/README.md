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

# Interactive Beam

## Overview

Interactive Beam is aimed at integrating Apache Beam with
[Jupyter notebook](http://jupyter.org/) to make pipeline prototyping and data
exploration much faster and easier. It provides nice features including

1.  Graphical representation

    When a pipeline is executed on a Jupyter notebook, it instantly displays the
    pipeline as a directed acyclic graph. Sampled PCollection results will be
    added to the graph as the pipeline execution proceeds.

2.  Fetching PCollections as list

    PCollections can be fetched as a list from the pipeline result. This unique
    feature of
    [InteractiveRunner](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/interactive/interactive_runner.py)
    makes it much easier to integrate Beam pipeline into data analysis.

    ```python
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    pcoll = p | SomePTransform | AnotherPTransform
    result = p.run().wait_until_finish()
    pcoll_list = result.get(pcoll)  # This returns a list!
    ```

3.  Faster re-execution

    [InteractiveRunner](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/interactive/interactive_runner.py)
    caches PCollection results of pipeline executed previously and re-uses it
    when the same pipeline is submitted again.

    ```python
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    pcoll = p | SomePTransform | AnotherPTransform
    result = p.run().wait_until_finish()

    pcoll2 = pcoll | YetAnotherPTransform
    result = p.run().wait_until_finish()  # <- only executes YetAnotherPTransform
    ```

## Status

*   Supported languages: Python
*   Supported platforms and caching location

    |                          | Caching locally | Caching on GCS |
    | ------------------------ | --------------- | -------------- |
    | Running on local machine | supported       | supported      |
    | Running on Flink         | supported       | supported      |

## Getting Started

**Note**: This guide assumes that you are somewhat familiar with key concepts in
Beam programming model including `Pipeline`, `PCollection`, `PTransform` and
`PipelineRunner` (see
[The Beam Model](https://github.com/apache/beam/tree/master#the-beam-model) for
a quick reference). For a more general and complete getting started guide, see
[Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/).

### Pre-requisites

*   Install [GraphViz](https://www.graphviz.org/download/) with your favorite
    system package manager.

-   Install [JupyterLab](https://jupyter.org/install.html). You can use
    either **conda** or **pip**.

    * conda
        ```bash
        conda install -c conda-forge jupyterlab
        ```
    * pip
        ```bash
        pip install jupyterlab
        ```

-   Install, create and activate your [venv](https://docs.python.org/3/library/venv.html).
    (optional but recommended)

    ```bash
    python3 -m venv /path/to/beam_venv_dir
    source /path/to/beam_venv_dir/bin/activate
    ```

    If you are using shells other than bash (e.g. fish, csh), check
    `beam_venv_dir/bin` for other scripts that activates the virtual
    environment.

    **CHECK** that the virtual environment is activated by running

    ```bash
    which python  # This sould point to beam_venv_dir/bin/python
    ```

*   Set up Apache Beam Python. **Make sure the virtual environment is activated
    when you run `setup.py`**

*   ```bash
    git clone https://github.com/apache/beam
    cd beam/sdks/python
    python setup.py install
    ```

-   Install an IPython kernel for the virtual environment you've just created.
    **Make sure the virtual environment is activate when you do this.** You can
    skip this step if not using venv.

    ```bash
    pip install ipykernel
    python -m ipykernel install --user --name beam_venv_kernel --display-name "Python3 (beam_venv)"
    ```

    **CHECK** that IPython kernel `beam_venv_kernel` is available for Jupyter to
    use.

    ```bash
    jupyter kernelspec list
    ```

-   Extend JupyterLab through labextension. **Note**: labextension is different from nbextension
    from pre-lab jupyter notebooks.

    All jupyter labextensions need nodejs

    ```bash
    # Homebrew users do
    brew install node
    # Or Conda users do
    conda install -c conda-forge nodejs
    ```

    Enable ipywidgets

    ```bash
    pip install ipywidgets
    jupyter labextension install @jupyter-widgets/jupyterlab-manager
    ```

### Start the notebook

To start the notebook, simply run

```bash
jupyter lab
```

Optionally increase the iopub broadcast data rate limit of jupyterlab

```bash
jupyter lab --NotebookApp.iopub_data_rate_limit=10000000
```


This automatically opens your default web browser pointing to
http://localhost:8888/lab.

You can create a new notebook file by clicking `Python3 (beam_venv)` from the launcher
page of jupyterlab.

Or after you've already opened a notebook, change the kernel by clicking
`Kernel` > `Change Kernel` > `Python3 (beam_venv)`.

Voila! You can now run Beam pipelines interactively in your Jupyter notebook!

In the notebook, you can use `tab` key on the keyboard for auto-completion.
To turn on greedy auto-completion, you can run such ipython magic

```
%config IPCompleter.greedy=True
```

You can also use `shift` + `tab` keys on the keyboard for a popup of docstrings at the
current cursor position.

**See [Interactive Beam Example.ipynb](examples/Interactive%20Beam%20Example.ipynb)
for more examples.**

## Portability

### Portability across Storage Systems

By default, the caches are kept on the local file system of the machine in
`/tmp` directory.

You can specify the caching directory as follows

```python
cache_dir = 'some/path/to/dir'
runner = interactive_runner.InteractiveRunner(cache_dir=cache_dir)
p = beam.Pipeline(runner=runner)
```

#### Caching PCollection on Google Cloud Storage

You can choose to cache PCollections on Google Cloud Storage with a few
credential settings.

*   Install [Google Cloud SDK](cloud.google.com/sdk), and set your project,
    account and other configurations with the following command.

    ```bash
    $ gcloud init
    $ gcloud auth login
    ```

*   Install the following google cloud modules. **Make sure the virtual
    environment is activated when you do this.**

*   ```bash
    $ python -m pip install --upgrade apache-beam[gcp]
    $ python -m pip install --upgrade google-cloud-storage
    $ python -m pip install --upgrade google-cloud-dataflow
    ```

*   Make sure you have **read and write access to that bucket** when you specify
    to use that directory as caching directory.

*   ```python
    cache_dir = 'gs://bucket-name/dir'
    runner = interactive_runner.InteractiveRunner(cache_dir=cache_dir)
    p = beam.Pipeline(runner=runner)
    ```

### Portability across Execution Platforms

The platform where the pipeline is executed is decided by the underlying runner
of InteractiveRunner. The default configuration runs on local machine with
[`direct_runner.DirectRunner()`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/direct/direct_runner.py)
as the underlying runner.

#### Running Interactive Beam on Flink

You can choose to run Interactive Beam on Flink with the following settings.

*   Use
    [`flink_runner.FlinkRunner()`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/portability/flink_runner.py)
    as the underlying runner.

    ```python
    p = beam.Pipeline(interactive_runner.InteractiveRunner(underlying_runner=flink_runner.FlinkRunner()))
    ```

**Note**: This guide and
[Interactive Beam Running on Flink.ipynb](examples/Interactive%20Beam%20Running%20on%20Flink.ipynb)
capture the status of the world when it's last updated.

## More Information

*   [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/)
*   [Interactive Beam Design Doc V2](https://docs.google.com/document/d/1DYWrT6GL_qDCXhRMoxpjinlVAfHeVilK5Mtf8gO6zxQ/edit?usp=sharing)
*   [Interactive Beam Design Doc V1](https://docs.google.com/document/d/10bTc97GN5Wk-nhwncqNq9_XkJFVVy0WLT4gPFqP6Kmw/edit?usp=sharing)
