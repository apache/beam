---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: python-development
description: Guides Python SDK development in Apache Beam, including environment setup, testing, building, and running pipelines. Use when working with Python code in sdks/python/.
---

# Python Development in Apache Beam

## Project Structure

### Key Directories
- `sdks/python/` - Python SDK root
  - `apache_beam/` - Main Beam package
    - `transforms/` - Core transforms (ParDo, GroupByKey, etc.)
    - `io/` - I/O connectors
    - `ml/` - Beam ML code (RunInference, etc.)
    - `runners/` - Runner implementations and wrappers
    - `runners/worker/` - SDK worker harness
  - `container/` - Docker container configuration
  - `test-suites/` - Test configurations
  - `scripts/` - Utility scripts

### Configuration Files
- `setup.py` - Package configuration
- `pyproject.toml` - Build configuration
- `tox.ini` - Test automation
- `pytest.ini` - Pytest configuration
- `.pylintrc` - Linting rules
- `.isort.cfg` - Import sorting
- `mypy.ini` - Type checking

## Environment Setup

### Using pyenv (Recommended)
```bash
# Install Python
pyenv install 3.X  # Use supported version from gradle.properties

# Create virtual environment
pyenv virtualenv 3.X beam-dev
pyenv activate beam-dev
```

### Install in Editable Mode
```bash
cd sdks/python
pip install -e .[gcp,test]
```

### Enable Pre-commit Hooks
```bash
pip install pre-commit
pre-commit install

# To disable
pre-commit uninstall
```

## Running Tests

### Unit Tests (filename: `*_test.py`)
```bash
# Run all tests in a file
pytest -v apache_beam/io/textio_test.py

# Run tests in a class
pytest -v apache_beam/io/textio_test.py::TextSourceTest

# Run a specific test
pytest -v apache_beam/io/textio_test.py::TextSourceTest::test_progress
```

### Integration Tests (filename: `*_it_test.py`)

#### On Direct Runner
```bash
python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDirectRunner'
```

#### On Dataflow Runner
```bash
# First build SDK tarball
pip install build && python -m build --sdist

# Run integration test
python -m pytest -o log_cli=True -o log_level=Info \
  apache_beam/ml/inference/pytorch_inference_it_test.py::PyTorchInference \
  --test-pipeline-options='--runner=TestDataflowRunner --project=<project>
                           --temp_location=gs://<bucket>/tmp
                           --sdk_location=dist/apache-beam-2.XX.0.dev0.tar.gz
                           --region=us-central1'
```

## Building Python SDK

### Build Source Distribution
```bash
cd sdks/python
pip install build && python -m build --sdist
# Output: sdks/python/dist/apache-beam-X.XX.0.dev0.tar.gz
```

### Build Wheel (faster installation)
```bash
./gradlew :sdks:python:bdistPy311linux  # For Python 3.11 on Linux
```

### Build SDK Container
```bash
./gradlew :sdks:python:container:py39:docker \
  -Pdocker-repository-root=gcr.io/your-project -Pdocker-tag=custom
```

## Running Pipelines with Modified Code

```bash
# Install modified SDK
pip install /path/to/apache-beam.tar.gz[gcp]

# Run pipeline
python my_pipeline.py \
  --runner=DataflowRunner \
  --sdk_location=/path/to/apache-beam.tar.gz \
  --project=my_project \
  --region=us-central1 \
  --temp_location=gs://my-bucket/temp
```

## Common Issues

### `NameError` when running DoFn
Global imports, functions, and variables in the main pipeline module are not serialized by default. Use:
```bash
--save_main_session
```

### Specifying Additional Dependencies
Use `--requirements_file=requirements.txt` or custom containers.

## Test Markers
- `@pytest.mark.it_postcommit` - Include in PostCommit test suite

## Gradle Commands for Python
```bash
# Run WordCount
./gradlew :sdks:python:wordCount

# Check environment
./gradlew :checkSetup
```

## Code Quality Tools
```bash
# Linting
pylint apache_beam/

# Type checking
mypy apache_beam/

# Formatting (via yapf)
yapf -i apache_beam/file.py

# Import sorting
isort apache_beam/file.py
```
