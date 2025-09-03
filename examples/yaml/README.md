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

## Example YAML Pipelines

A suite of YAML pipeline examples is currently located under the directory
[sdks/python/apache_beam/yaml/examples](../../sdks/python/apache_beam/yaml/examples).

### [Aggregation](../../sdks/python/apache_beam/yaml/examples/transforms/aggregation)

These examples leverage the built-in `Combine` transform for performing simple
aggregations including sum, mean, count, etc.

### [Blueprints](../../sdks/python/apache_beam/yaml/examples/transforms/blueprint)

These examples leverage DF or other existing templates and convert them to yaml
blueprints.

### [Element-wise](../../sdks/python/apache_beam/yaml/examples/transforms/elementwise)

These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`.

### [IO](../../sdks/python/apache_beam/yaml/examples/transforms/io)

These examples leverage the built-in IO transforms to read from and write to
various sources and sinks, including Iceberg, Kafka and Spanner.

### [Jinja](../../sdks/python/apache_beam/yaml/examples/transforms/jinja)

These examples use Jinja [templatization](https://beam.apache.org/documentation/sdks/yaml/#jinja-templatization)
to build off of different contexts and/or with different
configurations.

### [ML](../../sdks/python/apache_beam/yaml/examples/transforms/ml)

These examples include built-in ML-specific transforms such as `RunInference`,
`MLTransform` and `Enrichment`.
