---
type: languages
title: "Managing Python Pipeline Dependencies"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Managing Python Pipeline Dependencies

Dependency management is about specifying dependencies that your pipeline requires, and controlling which dependencies are used in production.


**Note:** Remote workers used for pipeline execution typically have a standard Python distribution installation in a Debian-based container image. If your code relies only on standard Python packages, then you probably don't need to do anything on this page.


## PyPI Dependencies {#pypi-dependencies}

If your pipeline uses public packages from the [Python Package Index](https://pypi.python.org/), you must make these packages available remotely on the workers.

For pipelines that consists only of a single Python file or a notebook, the most straightforward way to supply dependencies is to provide a
`requirements.txt` file. For more complex scenarios, define the [pipeline in a package](#multiple-file-dependencies) and consider installing your dependencies in a [custom container](#custom-containers).

To supply a requirements.txt file:

1. Find out which packages are installed on your machine. Run the following command:

        pip freeze > requirements.txt

    This command creates a `requirements.txt` file that lists all packages that are installed on your machine, regardless of where they were installed from.

2. Edit the `requirements.txt` file and delete all packages that are not relevant to your code.

3. Run your pipeline with the following command-line option:

        --requirements_file requirements.txt

    The runner will use the `requirements.txt` file to install your additional dependencies onto the remote workers.

> **NOTE**: As an alternative to `pip freeze`, use a library like [pip-tools](https://github.com/jazzband/pip-tools) to compile all of the dependencies required for the pipeline from a `requirements.in` file. In the `requirements.in` file, only the top-level dependencies are mentioned.

When you supply the `--requirements_file` pipeline option,  during pipeline submission, Beam downloads
the specified packages locally into a requirements cache directory,
and then stages the requirements cache directory to the runner.
At runtime, when available, Beam installs packages from the requirements cache.
This mechanism makes it possible to stage the dependency packages to the runner
at submission. At runtime, the runner workers might be able to install the
packages from the cache without needing a connection to PyPI. To disable staging the
requirements, use the `--requirements_cache=skip` pipeline option.
For more information, see the [help descriptions of these pipeline options](https://beam.apache.org/releases/pydoc/current/_modules/apache_beam/options/pipeline_options.html#SetupOptions).

## Custom Containers {#custom-containers}

You can pass a [container](https://hub.docker.com/search?q=apache%2Fbeam&type=image) image with all the dependencies that are needed for the pipeline. [Follow the instructions the show how to run the pipeline with custom container images](/documentation/runtime/environments/#running-pipelines).

1. If you are using a custom container image, we recommend that you install the dependencies from the `--requirements_file` directly into your image at build time. In this case, you do not need to pass `--requirements_file` option at runtime, which will reduce the pipeline startup time.

       # Add these lines with the path to the requirements.txt to the Dockerfile
       COPY <path to requirements.txt> /tmp/requirements.txt
       RUN python -m pip install -r /tmp/requirements.txt


## Local Python packages or non-public Python Dependencies {#local-or-nonpypi}

If your pipeline uses packages that are not available publicly (e.g. packages that you've downloaded from a GitHub repo), make these packages available remotely by performing the following steps:

1. Identify which packages are installed on your machine and are not public. Run the following command:

      pip freeze

    This command lists all packages that are installed on your machine, regardless of where they were installed from.

   1. Run your pipeline with the following command-line option:

           --extra_package /path/to/package/package-name

      where package-name is the package's tarball. You can build the package tarball using a command line tool called [build](https://setuptools.pypa.io/en/latest/userguide/quickstart.html#install-build).

            # Install build using pip
            pip install --upgrade build
            python -m build --sdist

      See the [build documentation](https://pypa-build.readthedocs.io/en/latest/index.html) for more details on this command.

## Multiple File Dependencies {#multiple-file-dependencies}

Often, your pipeline code spans multiple files. To run your project remotely, you must group these files as a Python package and specify the package when you run your pipeline. When the remote workers start, they will install your package. To group your files as a Python package and make it available remotely, perform the following steps:

1. Create a [setup.py](https://pythonhosted.org/an_example_pypi_project/setuptools.html) file for your project. The following is a very basic `setup.py` file.

        import setuptools

        setuptools.setup(
           name='PACKAGE-NAME',
           version='PACKAGE-VERSION',
           install_requires=[
             # List Python packages your pipeline depends on.
           ],
           packages=setuptools.find_packages(),
        )

2. Structure your project so that the root directory contains the `setup.py` file, the main workflow file, and a directory with the rest of the files, for example:

        root_dir/
          setup.py
          main.py
          my_package/
            my_pipeline_launcher.py
            my_custom_dofns_and_transforms.py
            other_utils_and_helpers.py

    See [Juliaset](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset) for an example that follows this project structure.

3. Install your package in the submission environment, for example by using the following command:

        pip install -e .

4. Run your pipeline with the following command-line option:

        --setup_file /path/to/setup.py

**Note:** It is not necessary to supply the `--requirements_file` [option](#pypi-dependencies) if the dependencies of your package are defined in the `install_requires` field of the `setup.py` file (see step 1).
However unlike with the `--requirements_file` option, when you use the `--setup_file` option, Beam doesn't stage the dependent packages to the runner.
Only the pipeline package is staged. If they aren't already provided in the runtime environment,
the package dependencies are installed from PyPI at runtime.


## Non-Python Dependencies or PyPI Dependencies with Non-Python Dependencies {#nonpython}

If your pipeline uses non-Python packages, such as packages that require installation using the `apt install` command, or uses a PyPI package that depends on non-Python dependencies during package installation, we recommend installing them using a [custom container](#custom-containers).
Otherwise, you must perform the following steps.

1. [Structure your pipeline as a package](#multiple-file-dependencies).

2. Add the required installation commands for the non-Python dependencies, such as the `apt install` commands, to the list of `CUSTOM_COMMANDS` in your `setup.py` file. See the [Juliaset setup.py file](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py) for an example.

    **Note:** You must verify that these commands run on the remote worker. For example, if you use `apt`, the remote worker needs `apt` support.

3. Run your pipeline with the following command-line option:

        --setup_file /path/to/setup.py

**Note:** Because custom commands execute after the dependencies for your workflow are installed (by `pip`), you should omit the PyPI package dependency from the pipeline's `requirements.txt` file and from the `install_requires` parameter in the `setuptools.setup()` call of your `setup.py` file.

## Pre-building SDK Container Image

In pipeline execution modes where a Beam runner launches SDK workers in Docker containers, the additional pipeline dependencies (specified via `--requirements_file` and other runtime options) are installed into the containers at runtime. This can increase the worker startup time.
However, it may be possible to pre-build the SDK containers and perform the dependency installation once before the workers start with `--prebuild_sdk_container_engine`. For instructions of how to use pre-building with Google Cloud
Dataflow, see [Pre-building the python SDK custom container image with extra dependencies](https://cloud.google.com/dataflow/docs/guides/using-custom-containers#prebuild).

**NOTE**: This feature is available only for the `Dataflow Runner v2`.

## Pickling and Managing the Main Session

When the Python SDK submits the pipeline for execution to a remote runner, the pipeline contents, such as transform user code, is serialized (or pickled) into a bytecode using
libraries that perform the serialization (also called picklers).  The default pickler library used by Beam is `dill`.
To use the `cloudpickle` pickler, supply the `--pickle_library=cloudpickle` pipeline option. The `cloudpickle` support is currently [experimental](https://github.com/apache/beam/issues/21298).

By default, global imports, functions, and variables defined in the main pipeline module are not saved during the serialization of a Beam job.
Thus, one might encounter an unexpected `NameError` when running a `DoFn` on any remote runner. To resolve this, supply the main session content with the pipeline by
setting the `--save_main_session` pipeline option. This will load the pickled state of the global namespace onto the Dataflow workers (if using `DataflowRunner`).
For example, see [Handling NameErrors](https://cloud.google.com/dataflow/docs/guides/common-errors#name-error) to set the main session on the `DataflowRunner`.

Managing the main session in Python SDK is only necessary when using `dill` pickler on any remote runner. Therefore, this issue will
not occur in `DirectRunner`.

Since serialization of the pipeline happens on the job submission, and deserialization happens at runtime, it is imperative that the same version of pickling library is used at job submission and at runtime.
To ensure this, Beam typically sets a very narrow supported version range for pickling libraries. If for whatever reason, users cannot use the version of `dill` or `cloudpickle` required by Beam, and choose to
install a custom version, they must also ensure that they use the same custom version at runtime (e.g. in their custom container,
or by specifying a pipeline dependency requirement).

## Control the dependencies the pipeline uses {#control-dependencies}

### Pipeline environments

To run a Python pipeline on a remote runner, Apache Beam translates the pipeline into a [runner-independent representation](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto) and submits it for execution. Translation happens in the **launch environment**. You can launch the pipeline from a Python virtual environment with the installed Beam SDK, or with tools like [Dataflow Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates), [Notebook environments](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development), [Apache Airflow](https://airflow.apache.org/), and more.

The [**runtime environment**](https://beam.apache.org/documentation/runtime/environments/) is the Python environment that a runner uses during pipeline execution. This environment is where the pipeline code runs to when it performs data  processing. The runtime environment includes Apache Beam and pipeline runtime dependencies.

### Create reproducible environments {#create-reproducible-environments}

You can use several tools to build reproducible Python environments:

* **Use [requirements files](https://pip.pypa.io/en/stable/user_guide/#requirements-files).**  After you install dependencies, generate the requirements file by using `pip freeze > requirements.txt`. To recreate an environment, install dependencies from the requirements.txt file by using `pip install -r requirements.txt`.

* **Use [constraint files](https://pip.pypa.io/en/stable/user_guide/#constraints-files).** You can use the constraint list to restrict the installation of packages, allowing only specified versions.

* **Use lock files.** Use dependency management tools like [PipEnv](https://pipenv.pypa.io/en/latest/), [Poetry](https://python-poetry.org/), and [pip-tools](https://github.com/jazzband/pip-tools) to specify top-level dependencies, to generate lock files of all transitive dependencies with pinned versions, and to create virtual environments from these lockfiles.

* **Use Docker container images.** You can package the launch and runtime environment inside a Docker container image. If the image includes all necessary dependencies, then the environment only changes when a container image is rebuilt.

Use version control for the configuration files that define the environment.

### Make the pipeline runtime environment reproducible

When a pipeline uses a reproducible runtime environment on a remote runner, the workers on the runner use the same dependencies each time the pipeline runs. A reproducible environment is immune to side-effects caused by releases of the pipeline's direct or transitive dependencies. It doesn’t require dependency resolution at runtime.

You can create a reproducible runtime environment in the following ways:

* Run your pipeline in a custom container image that has all dependencies for your pipeline. Use the `--sdk_container_image` pipeline option.

* Supply an exhaustive list of the pipeline's dependencies in the `--requirements_file` pipeline option. Use the `--prebuild_sdk_container_engine` option to perform the runtime environment initialization sequence before the pipeline execution. If your dependencies don't change, reuse the prebuilt image by using the `--sdk_container_image` option.

A self-contained runtime environment is usually reproducible. To check if the  runtime environment is self-contained, restrict internet access to PyPI in the pipeline runtime. If you use the Dataflow Runner, see the documentation for the [`--no_use_public_ips`](https://cloud.google.com/dataflow/docs/guides/routes-firewall#turn_off_external_ip_address) pipeline option.

If you need to recreate or upgrade the runtime environment, do so in a controlled way with visibility into changed dependencies:

* Do not modify container images when they are in use by running pipelines.

* Avoid using the tag `:latest` with your custom images. Tag your builds with a date or a unique identifier. If something goes wrong, using this type of tag might make it possible to revert the pipeline execution to a previously known working configuration and allow for an inspection of changes.

* Consider storing the output of `pip freeze` or the contents of `requirements.txt` in the version control system.

### Make the pipeline launch environment reproducible

The launch environment runs the **production version** of the pipeline. While developing the pipeline locally, you might use a **development environment** that includes dependencies for development, such as Jupyter or Pylint. The launch environment for production pipelines might not need these additional dependencies. You can construct and maintain it separately from the development environment.

To reduce side-effects on pipeline submissions, it is best to able to [recreate the launch environment in a reproducible manner](#create-reproducible-environments).

[Dataflow Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) provide an example of a containerized, reproducible launch environment.

To create reproducible installations of Beam into a clean virtual environment, use [requirements files](https://pip.pypa.io/en/stable/user_guide/#requirements-files) that list all Python dependencies included in Beam's default container images constraint files:

```
BEAM_VERSION=2.48.0
PYTHON_VERSION=`python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"`
pip install apache-beam==$BEAM_VERSION --constraint https://raw.githubusercontent.com/apache/beam/release-${BEAM_VERSION}/sdks/python/container/py${PY_VERSION}/base_image_requirements.txt
```

Use a constraint file to ensure that Beam dependencies in the launch environment match the versions in default Beam containers. A constraint file might also remove the need for dependency resolution at installation time.

### Make the launch environment compatible with the runtime environment

The launch environment translates the  pipeline graph into a [runner-independent representation](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto). This process involves serializing (or pickling) the code of the transforms. The serialized content is deserialized on the workers. If the runtime worker environment significantly differs from the launch environment, runtime errors might occur for the following reasons:

* The Apache Beam version  must match in the submission and runtime environments. Python major.minor versions must match as well. Otherwise, the pipeline might fail with errors like `Pipeline construction environment and pipeline runtime environment are not compatible`. On older SDK versions, the error might be reported as `SystemError: unknown opcode`.

* Versions of `protobuf` in the submission and runtime environment need to match or be compatible.

* Libraries used in the pipeline code might need to match. If serialized pipeline code has references to functions or modules that aren’t available on the workers, the pipeline might fail with `ModuleNotFound` or `AttributeError` exceptions on the remote runner. If you encounter such errors, make sure that the affected libraries are available on the remote worker, and check whether you need to [save the main session](  https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pickling-and-managing-the-main-session).

* The version of the pickling library used at submission time must match the version installed at runtime. To enforce this, Beam sets a tight bounds on the version of serializer libraries (dill and cloudpickle). You can force install a different version of `dill` or `cloudpickle` than required by Beam under the following conditions:
  * You install the same version in submission and in the runtime environment.
  * The chosen version works for your pipeline.

To check whether the runtime environment matches the launch environment, inspect differences in the `pip freeze` output in both environments. Update to the latest version of Beam, because environment compatibility checks are included in newer SDK versions.

Finally, you can use the same environment by launching the pipeline from the  containerized environment that you use at runtime. [Dataflow Flex templates built from a custom container image](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#use_custom_container_images) offer this setup. In this scenario, you can recreate both launch and runtime environments in a reproducible manner. Because both containers are created from the same image, the launch and runtime environments are compatible with each other by default.
