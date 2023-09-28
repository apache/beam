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

# Unrecoverable Errors in Beam Python

## What is an Unrecoverable Error?

An unrecoverable error is an issue at job start-up time that will
prevent a job from ever running successfully, usually due to some kind
of misconfiguration. Solving these issues when they occur is key to
successfully running a Beam Python pipeline.

## Common Unrecoverable Errors

### Job Submission/Runtime Python Version Mismatch

If the Python version used for job submission does not match the
Python version used to build the worker container, the job will not
execute. Ensure that the Python version being used for job submission
and the container Python version match.

### PIP Dependency Resolution Failures

During worker start-up, dependencies are checked and installed in
the worker container before accepting work. If a pipeline requires
additional dependencies not already present in the runtime environment,
they are installed here. If there’s an issue during this process
(e.g. a dependency version cannot be found, or a worker cannot
connect to PyPI) the worker will fail and may try to restart
depending on the runner. Ensure that dependency versions provided in
your requirements.txt file exist and can be installed locally before
submitting jobs.

### Dependency Version Mismatches

When additional dependencies like `torch`, `transformers`, etc. are not
specified via a requirements_file or preinstalled in a custom container
then the worker might fail to deserialize (unpickle) the user code.
This can result in `ModuleNotFound` errors. If dependencies are installed
but their versions don't match the versions in submission environment,
pipeline might have `AttributeError` messages.

Ensure that the required dependencies at runtime and in the submission
environment are the same along with their versions. For better visibility,
debug logs are added specifying the dependencies at both stages starting in
Beam 2.52.0. For more information, see: https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#control-dependencies