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

name: yaml-development
description: Guides YAML SDK development in Apache Beam, including environment setup, testing, and key concepts. Use when working with Beam YAML code in sdks/python/apache_beam/yaml/.
---

# YAML Development in Apache Beam

## Project Structure

### Key Files in `sdks/python/apache_beam/yaml/`
- `yaml_transform.py` - Core YAML expansion logic, parsing, and translation to Beam pipelines.
- `yaml_io.py` - Python implementations for builtin IOs (PubSub, BigQuery, Iceberg, etc.).
- `yaml_provider.py` - Manages providers (Python, Java cross-language) that implement transforms.
- `integration_tests.py` - Runs integration tests defined in YAML files or using testcontainers.
- `standard_io.yaml` - Declarations of standard IO transforms and their mappings to providers.
- `standard_providers.yaml` - Configuration for standard providers (e.g., Java expansion services).
## Environment Setup
Since Beam YAML is implemented within the Python SDK, the environment setup is identical to Python development. Refer to the `python-development` skill for details on using `pyenv` and installing in editable mode (`pip install -e .[gcp,test]`).

## Running YAML Pipelines

You can run Beam YAML pipelines using the `main.py` script in the YAML directory.

### Using `main.py` directly
```bash
python -m apache_beam.yaml.main --yaml_pipeline_file=/path/to/pipeline.yaml [pipeline_options]
```

### Example: Running locally
```bash
python -m apache_beam.yaml.main \
  --yaml_pipeline_file=sdks/python/apache_beam/yaml/examples/simple_filter.yaml \
  --runner=DirectRunner
```

### Example: Running on Dataflow
```bash
python -m apache_beam.yaml.main \
  --yaml_pipeline_file=sdks/python/apache_beam/yaml/examples/simple_filter.yaml \
  --runner=DataflowRunner \
  --project=my-project \
  --region=us-central1 \
  --temp_location=gs://my-bucket/temp
```

## Running Tests

### Unit Tests
Beam YAML has extensive unit tests covering parsing, expansion, and specific transforms.
```bash
# Run all tests in a file
pytest sdks/python/apache_beam/yaml/yaml_transform_test.py

# Run a specific test
pytest sdks/python/apache_beam/yaml/yaml_transform_test.py::YamlTransformTest::test_simple_pipeline
```

### Integration Tests
Integration tests often spin up Docker containers (via `testcontainers`) for external services like MongoDB, Kafka, or databases.
```bash
# Run integration tests matching a specific keyword (e.g., mongodb)
pytest sdks/python/apache_beam/yaml/integration_tests.py -k mongodb
```

## Key Concepts

### Providers
Beam YAML uses "providers" to find implementations for transforms requested in the YAML file.
- **Inline/Python Providers**: Leverage Python functions or PTransforms directly.
- **Java/External Providers**: Use Beam's cross-language capabilities to invoke Java transforms via an expansion service.

### Preprocessing
Before execution, a YAML pipeline is preprocessed to resolve schemas, match transforms to providers, and expand shorthand notations (like `chain` or `source`/`sink` composites).

## Common Issues

### Cross-language Failures
If a test requires a Java transform, ensure that:
1. Docker is running (if using testcontainers).
2. The correct expansion service is available or can be started.
3. Java environment is correctly configured (sometimes requires specific Java versions like Java 17/21).

### Schema Mismatches
YAML relies heavily on Beam schemas. Ensure that fields produced by a transform match the fields expected by the next transform. Use explicit mapping if necessary.
