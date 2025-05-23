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

# Examples Catalog

<!-- TOC -->
* [Examples Catalog](#examples-catalog)
  * [Wordcount](#wordcount)
  * [Transforms](#transforms)
    * [Aggregation](#aggregation)
    * [Blueprints](#blueprints)
    * [Element-wise](#element-wise)
    * [IO](#io)
    * [ML](#ml)

<!-- TOC -->

## Prerequistes
Build this jar for running with the run command in the next stage:
```
cd <path_to_beam_repo>/beam; ./gradlew sdks:java:io:google-cloud-platform:expansion-service:shadowJar
```

## Example Run
This module contains a series of Beam YAML code samples that can be run using
the command:
```
python -m apache_beam.yaml.main --pipeline_spec_file=/path/to/example.yaml
```

Depending on the yaml pipeline, the output may be emitted to standard output or
a file located in the execution folder used.

## Wordcount
A good starting place is the [Wordcount](wordcount_minimal.yaml) example under
the root example directory.
This example reads in a text file, splits the text on each word, groups by each
word, and counts the occurrence of each word. This is a classic example used in
the other SDK's and shows off many of the functionalities of Beam YAML.

## Testing
A test file is located in the testing folder that will execute all the example
yamls and confirm the expected results.
```
pytest -v testing/

or

python -m unittest -v testing/examples_test.py
```

## Transforms

Examples in this directory show off the various built-in transforms of the Beam
YAML framework.

### Aggregation
These examples leverage the built-in `Combine` transform for performing simple
aggregations including sum, mean, count, etc.

### Blueprints
These examples leverage DF or other existing templates and convert them to yaml
blueprints.

### Element-wise
These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`. More information can be found about mapping transforms
[here](https://beam.apache.org/documentation/sdks/yaml-udf/).

### IO
These examples leverage the built-in `Spanner_Read` and `Spanner_Write`
transform for performing simple reads and writes from a spanner DB.

### ML
These examples leverage the built-in `Enrichment` transform for performing
ML enrichments.

More information can be found about aggregation transforms
[here](https://beam.apache.org/documentation/sdks/yaml-combine/).