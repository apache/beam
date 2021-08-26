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

> **Note:** This page is only applicable to runners that do remote execution.

When you run your pipeline locally, the packages that your pipeline depends on are available because they are installed on your local machine. However, when you want to run your pipeline remotely, you must make sure these dependencies are available on the remote machines. This tutorial shows you how to make your dependencies available to the remote workers. Each section below refers to a different source that your package may have been installed from.

**Note:** Remote workers used for pipeline execution typically have a standard Python distribution installation in a Debian-based container image. If your code relies only on standard Python packages, then you probably don't need to do anything on this page.


## PyPI Dependencies {#pypi-dependencies}

If your pipeline uses public packages from the [Python Package Index](https://pypi.python.org/), make these packages available remotely by performing the following steps:

**Note:** If your PyPI package depends on a non-Python package (e.g. a package that requires installation on Linux using the `apt-get install` command), see the [PyPI Dependencies with Non-Python Dependencies](#nonpython) section instead.

1. Find out which packages are installed on your machine. Run the following command:

        pip freeze > requirements.txt

    This command creates a `requirements.txt` file that lists all packages that are installed on your machine, regardless of where they were installed from.

2. Edit the `requirements.txt` file and leave only the packages that were installed from PyPI and are used in the workflow source. Delete all packages that are not relevant to your code.

3. Run your pipeline with the following command-line option:

        --requirements_file requirements.txt

    The runner will use the `requirements.txt` file to install your additional dependencies onto the remote workers.

**Important:** Remote workers will install all packages listed in the `requirements.txt` file. Because of this, it's very important that you delete non-PyPI packages from the `requirements.txt` file, as stated in step 2. If you don't remove non-PyPI packages, the remote workers will fail when attempting to install packages from sources that are unknown to them.


## Local or non-PyPI Dependencies {#local-or-nonpypi}

If your pipeline uses packages that are not available publicly (e.g. packages that you've downloaded from a GitHub repo), make these packages available remotely by performing the following steps:

1. Identify which packages are installed on your machine and are not public. Run the following command:

        pip freeze

    This command lists all packages that are installed on your machine, regardless of where they were installed from.

2. Run your pipeline with the following command-line option:

        --extra_package /path/to/package/package-name

   where package-name is the package's tarball. If you have the `setup.py` for that
   package then you can build the tarball with the following command:

        python setup.py sdist

   See the [sdist documentation](https://docs.python.org/2/distutils/sourcedist.html) for more details on this command.

## Multiple File Dependencies

Often, your pipeline code spans multiple files. To run your project remotely, you must group these files as a Python package and specify the package when you run your pipeline. When the remote workers start, they will install your package. To group your files as a Python package and make it available remotely, perform the following steps:

1. Create a [setup.py](https://pythonhosted.org/an_example_pypi_project/setuptools.html) file for your project. The following is a very basic `setup.py` file.

        import setuptools

        setuptools.setup(
           name='PACKAGE-NAME',
           version='PACKAGE-VERSION',
           install_requires=[],
           packages=setuptools.find_packages(),
        )

2. Structure your project so that the root directory contains the `setup.py` file, the main workflow file, and a directory with the rest of the files.

        root_dir/
          setup.py
          main.py
          other_files_dir/

    See [Juliaset](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset) for an example that follows this required project structure.

3. Run your pipeline with the following command-line option:

        --setup_file /path/to/setup.py

**Note:** If you [created a requirements.txt file](#pypi-dependencies) and your project spans multiple files, you can get rid of the `requirements.txt` file and instead, add all packages contained in `requirements.txt` to the `install_requires` field of the setup call (in step 1).


## Non-Python Dependencies or PyPI Dependencies with Non-Python Dependencies {#nonpython}

If your pipeline uses non-Python packages (e.g. packages that require installation using the `apt-get install` command), or uses a PyPI package that depends on non-Python dependencies during package installation, you must perform the following steps.

1. Add the required installation commands (e.g. the `apt-get install` commands) for the non-Python dependencies to the list of `CUSTOM_COMMANDS` in your `setup.py` file. See the [Juliaset setup.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py) for an example.

    **Note:** You must make sure that these commands are runnable on the remote worker (e.g. if you use `apt-get`, the remote worker needs `apt-get` support).

2. If you are using a PyPI package that depends on non-Python dependencies, add `['pip', 'install', '<your PyPI package>']` to the list of `CUSTOM_COMMANDS` in your `setup.py` file.

3. Structure your project so that the root directory contains the `setup.py` file, the main workflow file, and a directory with the rest of the files.

        root_dir/
          setup.py
          main.py
          other_files_dir/

    See the [Juliaset](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset) project for an example that follows this required project structure.

4. Run your pipeline with the following command-line option:

        --setup_file /path/to/setup.py

**Note:** Because custom commands execute after the dependencies for your workflow are installed (by `pip`), you should omit the PyPI package dependency from the pipeline's `requirements.txt` file and from the `install_requires` parameter in the `setuptools.setup()` call of your `setup.py` file.
