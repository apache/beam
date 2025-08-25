---
type: languages
title: "Apache Beam YAML Schema"
---
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

# Beam YAML Schema

As pipelines grow in size and complexity, it becomes more common to encounter
data that is malformed, doesn't meet preconditions, or otherwise causes issues
during processing.

Beam YAML helps the user detect and capture these issues by using the optional
`output_schema` configuration, which is available for any transform in the YAML
SDK. For example, the following code creates a few "good" records and specifies
that the output schema from the `Create` transform should have records that
follow the expected schema: `sdk` as a string and `year` as an integer.

```yaml
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - {sdk: MapReduce, year: 2004}
          - {sdk: MillWheel, year: 2008}
        output_schema:
          type: object
          properties:
            sdk:
              type: string
            year:
              type: integer
    - type: AssertEqual
      config:
        elements:
          - {sdk: MapReduce, year: 2004}
          - {sdk: MillWheel, year: 2008}
```

More than likely though a user will want to detect errors with schemas like
that and thats where tagging an additional `error_handling` config inside the
`output_schema` config comes into play. For example, the following code will
create a few "good" and "bad" records with a specified schema of `sdk` as a
string and `year` as an integer with error_handling output going to invalid
rows. An additonal `MapToFields` transform will take the error_handling output
and capture the element data as rows. Two `AssertEqual` transforms will verify
the "good" and "bad" rows accordingly.

```yaml
pipeline:
  type: composite
  transforms:
    - type: Create
      config:
        elements:
          - {sdk: MapReduce, year: 2004}
          - {sdk: CheeseWheel, year: "apple"}
          - {sdk: MillWheel, year: 2008}
        output_schema:
          type: object
          properties:
            sdk:
              type: string
            year:
              type: integer
          error_handling:
            output: invalid_rows
    - type: MapToFields
      input: Create.invalid_rows
      config:
        language: python
        fields:
          sdk: "element.sdk"
          year: "element.year"
    - type: AssertEqual
      input: MapToFields
      config:
        elements:
          - {sdk: CheeseWheel, year: "apple"}
    - type: AssertEqual
      input: Create
      config:
        elements:
          - {sdk: MapReduce, year: 2004}
          - {sdk: MillWheel, year: 2008}
```

For more detailed information on error handling, see this [page](https://beam.apache.org/documentation/sdks/yaml-errors/).
