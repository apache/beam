---
type: languages
title: "Apache Beam YAML Error Handling"
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

# Beam YAML Error Handling

The larger one's pipeline gets, the more common it is to encounter "exceptional"
data that is malformatted, doesn't handle the proper preconditions, or otherwise
breaks during processing.  Generally any such record will cause the pipeline to
permanently fail, but often it is desirable to allow the pipeline to continue,
re-directing bad records to another path for special handling or simply
recording them for later off-line analysis.  This is often called the
"dead letter queue" pattern.

Beam YAML has special support for this pattern if the transform supports a
`error_handling` config parameter with an `output` field.
The `output` parameter is a name that must referenced as an input to
another transform that will process the errors (e.g. by writing them out).
For example,
the following code will write all "good" processed records to one file and
any "bad" records to a separate file.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv

    - type: MapToFields
      input: ReadFromCsv
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          ratio: col2 / col3
        error_handling:
          output: my_error_output

    - type: WriteToJson
      input: MapToFields
      config:
        path: /path/to/output.json

    - type: WriteToJson
      name: WriteErrorsToJson
      input: MapToFields.my_error_output
      config:
        path: /path/to/errors.json
```

Note that with `error_handling` declared, `MapToFields.my_error_output`
**must** be consumed; to ignore it will be an error.  Any use is fine, e.g.
logging the bad records to stdout would be sufficient (though not recommended
for a robust pipeline).

Note also that the exact format of the error outputs is still being finalized.
They can be safely printed and written to outputs, but their precise schema
may change in a future version of Beam and should not yet be depended on.

Some transforms allow for extra arguments in their error_handling config, e.g.
for Python functions one can give a `threshold` which limits the relative number
of records that can be bad before considering the entire pipeline a failure

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv

    - type: MapToFields
      input: ReadFromCsv
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          ratio: col2 / col3
        error_handling:
          output: my_error_output
          # If more than 10% of records throw an error, stop the pipeline.
          threshold: 0.1

    - type: WriteToJson
      input: MapToFields
      config:
        path: /path/to/output.json

    - type: WriteToJson
      name: WriteErrorsToJson
      input: MapToFields.my_error_output
      config:
        path: /path/to/errors.json
```

One can do arbitrary further processing on these failed records if desired,
e.g.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv

    - type: MapToFields
      name: ComputeRatio
      input: ReadFromCsv
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          ratio: col2 / col3
        error_handling:
          output: my_error_output

    - type: MapToFields
      name: ComputeRatioForBadRecords
      input: ComputeRatio.my_error_output
      config:
        language: python
        fields:
          col1: col1
          ratio: col2 / (col3 + 1)
        error_handling:
          output: still_bad

    - type: WriteToJson
      # Takes as input everything from the "success" path of both transforms.
      input: [ComputeRatio, ComputeRatioForBadRecords]
      config:
        path: /path/to/output.json

    - type: WriteToJson
      name: WriteErrorsToJson
      # These failed the first and the second transform.
      input: ComputeRatioForBadRecords.still_bad
      config:
        path: /path/to/errors.json
```

When using the `chain` syntax, the required error consumption can happen
in an `extra_transforms` block.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv

    - type: MapToFields
      name: SomeStep
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          ratio: col2 / col3
        error_handling:
          output: errors

    - type: MapToFields
      name: AnotherStep
      config:
        language: python
        fields:
          col1: col1
          # This could raise a divide-by-zero error.
          inverse_ratio: 1 / ratio
        error_handling:
          output: errors

    - type: WriteToJson
      config:
        path: /path/to/output.json

  extra_transforms:
    - type: WriteToJson
      name: WriteErrors
      input: [SomeStep.errors, AnotherStep.errors]
      config:
        path: /path/to/errors.json
```
