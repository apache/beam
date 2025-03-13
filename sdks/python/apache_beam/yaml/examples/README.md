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
    * [Element-wise](#element-wise)
    * [Aggregation](#aggregation)
<!-- TOC -->

This module contains a series of Beam YAML code samples that can be run using
the command:
```
python -m apache_beam.yaml.main --pipeline_spec_file=/path/to/example.yaml
```

## Wordcount
A good starting place is the [Wordcount](wordcount_minimal.yaml) example under
the root example directory.
This example reads in a text file, splits the text on each word, groups by each
word, and counts the occurrence of each word. This is a classic example used in
the other SDK's and shows off many of the functionalities of Beam YAML.

## Transforms

Examples in this directory show off the various built-in transforms of the Beam
YAML framework.

### Element-wise
These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`. More information can be found about mapping transforms
[here](https://beam.apache.org/documentation/sdks/yaml-udf/).

### Aggregation
These examples leverage the built-in `Combine` transform for performing simple
aggregations including sum, mean, count, etc.

More information can be found about aggregation transforms
[here](https://beam.apache.org/documentation/sdks/yaml-combine/).