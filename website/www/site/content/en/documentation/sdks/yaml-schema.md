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

However, a user will more likely want to detect and handle schema errors. If a
transform has a built-in error_handling configuration, the user can specify that
error_handling configuration and any errors found will be appended to the
transform error_handling output. For example, the following code will
create a few "good" and "bad" records with a specified schema of `user` as a
string and `timestamp` as a boolean. The `alice` row will fail in the standard
way because of not being an integer for the AssignTimestamps transform, while
the `bob` row will fail because after the AssignTimestamp transformation, the
output row will have the timestamp as an integer when it should be a boolean.


```yaml
pipeline:
  type: composite
  transforms:
    - type: Create
      name: CreateVisits
      config:
        elements:
          - {user: alice, timestamp: "not-valid"}
          - {user: bob, timestamp: 3}
    - type: AssignTimestamps
      input: CreateVisits
      config:
        timestamp: timestamp
        error_handling:
          output: invalid_rows
        output_schema:
          type: object
          properties:
            user:
              type: string
            timestamp:
              type: boolean
    - type: MapToFields
      name: ExtractInvalidTimestamp
      input: AssignTimestamps.invalid_rows
      config:
        language: python
        fields:
          user: "element.user"
          timestamp: "element.timestamp"
    - type: AssertEqual
      input: ExtractInvalidTimestamp
      config:
        elements:
          - {user: "alice", timestamp: "not-valid"}
          - {user: bob, timestamp: 3}
    - type: AssertEqual
      input: AssignTimestamps
      config:
        elements: []
```

WARNING: If a transform doesn't have the error_handling configuration available
and a user chooses to use this optional output_schema feature, any failures
found will result in the entire pipeline failing. If the user would still like
to have some kind of output schema validation, please use the ValidateWithSchema
transform instead.

NOTE: When using the output_schema config, the main output key to validate on
will be determined based on these criteria:

  1. An output with the key 'output'.
  2. An output with the key 'good'.
  3. The single output if there is only one.

Failures will result if the main output cannot be determined because there are
multiple outputs and none are named 'output' or 'good'.



For more detailed information on error handling, see this [page](https://beam.apache.org/documentation/sdks/yaml-errors/).
