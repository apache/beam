---
type: languages
title: "Unrecoverable Errors in Beam Python"
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

# Unrecoverable errors in Beam Python

Unrecoverable errors are issues that occur at job start-up time and
prevent jobs from ever running successfully. The problem usually stems
from a misconfiguration. This page provides context about
common errors and troubleshooting information.

## Job submission or Python runtime version mismatch {#python-version-mismatch}

If the Python version that you use to submit your job doesn't match the
Python version used to build the worker container, the job doesn't run.
The job fails immediately after job submission.

To resolve this issue, ensure that the Python version used to submit the job
matches the Python container version.

## Dependency resolution failures with pip {#dependency-resolution-failures}

During worker start-up, the worker might fail and, depending on the
runner, try to restart.

Before workers accept work, dependencies are checked and installed in
the worker container. If a pipeline requires
dependencies not already present in the runtime environment,
they are installed at this time.
When a problem occurs during this process, you might encounter
dependency resolution failures.

Examples of problems include the following:

- A dependency version can't be found.
- A worker can't connect to PyPI.

To resolve this issue, before submitting your job, ensure that the
dependency versions provided in your `requirements.txt` file exist
and that you can install them locally.

## Dependency version mismatches {#dependency-version}

When your pipeline has dependency version mismatches, you might
see `ModuleNotFound` errors or `AttributeError` messages.

 - The `ModuleNotFound` errors occur when additional dependencies,
   such as `torch` and `transformers`, are neither specified in a
   `requirements_file` nor preinstalled in a custom container.
	In this case, the worker might fail to deserialize (unpickle) the user code.

- Your pipeline might have `AttributeError` messages when dependencies
  are installed but their versions don't match the versions in submission environment.

To resolve these problems, ensure that the required dependencies and their versions are the same
at runtime and in the submission environment. To help you identify these issues,
in Apache Beam 2.52.0 and later versions, debug logs specify the dependencies at both stages.
For more information, see
[Control the dependencies the pipeline uses](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#control-dependencies).