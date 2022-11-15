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

    Visualize the Pipeline DAG:

    ```python
    import apache_beam.runners.interactive.interactive_beam as ib
    from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

    p = beam.Pipeline(InteractiveRunner())
    # ... add transforms
    ib.show_graph(pipeline)
    ```

    Visualize elements in a PCollection:

    ```python
    pcoll = p | beam.Create([1, 2, 3])
    # include_window_info displays windowing information
    # visualize_data visualizes data with https://pair-code.github.io/facets/
    ib.show(pcoll, include_window_info=True, visualize_data=True)
    ```
    More details see the docstrings of `interactive_beam` module.

2.  Support of streaming record/replay and dynamic visualization

    For streaming pipelines, Interactive Beam records a subset of unbounded
    sources in the pipeline automatically so that they can be replayed for
    pipeline changes during prototyping.

    There are a few knobs to tune the source recording:

    ```python
    # Set the amount of time recording data from unbounded sources.
    ib.options.recording_duration = '10m'

    # Set the recording size limit to 1 GB.
    ib.options.recording_size_limit = 1e9

    # Visualization is dynamic as data streamed in real time.
    # n=100 indicates that displays at most 100 elements.
    # duration=60 indicates that displays at most 60 seconds worth of unbounded
    # source generated data.
    ib.show(pcoll, include_window_info=True, n=100, duration=60)

    # duration can also be strings.
    ib.show(pcoll, include_window_info=True, duration='1m')

    # If neither n nor duration is provided, the display is indefinitely until
    # the current machine's recording usage hits the threadshold set by
    # ib.options.
    ib.show(pcoll, include_window_info=True)
    ```
    More details see the docstrings of `interactive_beam` module.

3.  Fetching PCollections as pandas.DataFrame

    PCollections can be collected as a pandas.DataFrame:

    ```python
    pcoll_df = ib.collect(pcoll)  # This returns a pandas.DataFrame!
    ```

4.  Faster execution and re-execution

    Interactive Beam analyzes the pipeline graph depending on what PCollection
    you want to inspect and builds a pipeline fragment to only compute
    necessary data.

    ```python
    pcoll = p | PTransformA | PTransformB
    pcoll2 = p | PTransformC | PTransformD

    ib.collect(pcoll)  # <- only executes PTransformA and PTransformB
    ib.collect(pcoll2)  # <- only executes PTransformC and PTransformD
    ```

    Interactive Beam caches PCollection inspected previously and re-uses it
    when the data is still in scope.

    ```python
    pcoll = p | PTransformA
    # pcoll2 depends on pcoll
    pcoll2 = pcoll | PTransformB
    ib.collect(pcoll2)  # <- caches data for both pcoll and pcoll2

    pcoll3 = pcoll2 | PTransformC
    ib.collect(pcoll3)  # <- reuses data of pcoll2 and only executes PTransformC

    pcoll4 = pcoll | PTransformD
    ib.collect(pcoll4)  # <- reuses data of pcoll and only executes PTransformD
    ```

5. Supports global and local scopes

   Interactive Beam automatically watches the `__main__` scope for pipeline and
   PCollection definitions to implicitly do magic under the hood.

   ```python
   # In a script or in a notebook
   p = beam.Pipeline(InteractiveRunner())
   pcoll = beam | SomeTransform
   pcoll2 = pcoll | SomeOtherTransform

   # p, pcoll and pcoll2 are all known to Interactive Beam.
   ib.collect(pcoll)
   ib.collect(pcoll2)
   ib.show_graph(p)
   ```

   You have to explicitly watch pipelines and PCollections in your local scope.
   Otherwise, Interactive Beam doesn't know about them and won't handle them
   with interactive features.

   ```python
   def a_func():
     p = beam.Pipeline(InteractiveRunner())
     pcoll = beam | SomeTransform
     pcoll2 = pcoll | SomeOtherTransform

     # Watch everything defined locally before this line.
     ib.watch(locals())
     # Or explicitly watch them.
     ib.watch({
         'p': p,
         'pcoll': pcoll,
         'pcoll2': pcoll2})

     # p, pcoll and pcoll2 are all known to Interactive Beam.
     ib.collect(pcoll)
     ib.collect(pcoll2)
     ib.show_graph(p)

     return p, pcoll, pcoll2

   # Or return them to main scope
   p, pcoll, pcoll2 = a_func()
   ib.collect(pcoll)  # Also works!
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

*   Install, create and activate your [venv](https://docs.python.org/3/library/venv.html).
    (optional but recommended)

    ```bash
    python3 -m venv /path/to/beam_venv_dir
    source /path/to/beam_venv_dir/bin/activate
    pip install --upgrade pip setuptools wheel
    ```

    If you are using shells other than bash (e.g. fish, csh), check
    `beam_venv_dir/bin` for other scripts that activates the virtual
    environment.

    **CHECK** that the virtual environment is activated by running

    ```bash
    which python  # This sould point to beam_venv_dir/bin/python
    ```

*   Install [JupyterLab](https://jupyter.org/install.html). You can use
    either **conda** or **pip**.

    * conda
        ```bash
        conda install -c conda-forge jupyterlab
        ```
    * pip
        ```bash
        pip install jupyterlab
        ```

*   Set up Apache Beam Python. **Make sure the virtual environment is activated
    when you run `setup.py`**

*   ```bash
    git clone https://github.com/apache/beam
    cd beam/sdks/python
    python setup.py install
    ```

*   Install an IPython kernel for the virtual environment you've just created.
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

*   Extend JupyterLab through labextension. **Note**: labextension is different from nbextension
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
ib.options.cache_root = 'some/path/to/dir'
```

When using an `InteractiveRunner(underlying_runner=...)` that is running remotely
and distributed, a distributed file system such as Cloud Storage
(`ib.options.cache_root = gs://bucket/obj`) is necessary.

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

*   You may configure a cache directory to be used by all interactive pipelines through
    using the `cache_root` option under interactive_beam. If the cache directory is specified
    this way, no additional parameters are required to be passed in during pipeline instantiation.

*   ```python
    ib.options.cache_root = 'gs://bucket-name/obj'
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

*   Alternatively, if the runtime environment is configured with a Google Cloud project, you can run Interactive Beam with Flink on Cloud Dataproc. To do so, configure the pipeline with a Google Cloud project. If using dev versioned Beam built from source code, it is necessary to specify an `environment_config` option to configure a containerized Beam SDK (you can choose a released container or build one yourself).

*   ```python
    ib.options.cache_root = 'gs://bucket-name/dir'
    options = PipelineOptions([
    # The project can be attained simply from running the following commands:
    # import google.auth
    # project = google.auth.default()[1]
    '--project={}'.format(project),
    # The following environment_config only needs to be used when using a development kernel.
    # Users do not need to use the 2.35.0 SDK, but the chosen release must be compatible with
    # the Flink version used by the Dataproc image used by Interactive Beam. The current Flink
    # version used is 1.12.5.
    '--environment_config=apache/beam_python3.7_sdk:2.35.0',
    ])
    ```

**Note**: This guide and
[Interactive Beam Running on Flink.ipynb](examples/Interactive%20Beam%20Running%20on%20Flink.ipynb)
capture the status of the world when it's last updated.

## More Information

*   [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/)
*   [Interactive Beam Design Doc V2](https://docs.google.com/document/d/1DYWrT6GL_qDCXhRMoxpjinlVAfHeVilK5Mtf8gO6zxQ/edit?usp=sharing)
*   [Interactive Beam Design Doc V1](https://docs.google.com/document/d/10bTc97GN5Wk-nhwncqNq9_XkJFVVy0WLT4gPFqP6Kmw/edit?usp=sharing)
