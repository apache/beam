---
type: languages
title: "Apache Beam YAML Testing"
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

# Beam YAML Tests

A robust testing story is an important part of any production setup.
Though the various built-in (and externally provided) transform in a Beam YAML
pipeline can be expected to be well tested, it can be important to have tests
that ensure the pipeline as a whole behaves as expected.  This is particularly
true for transforms that contain non-trivial UDF logic.

## Whole pipeline tests

For example, consider the example word count pipeline.

```
pipeline:
  transforms:
  - type: ReadFromText
    name: Read from GCS
    config:
      path: gs://dataflow-samples/shakespeare/kinglear.txt
  - type: MapToFields
    name: Split words
    config:
      language: python
      fields:
        word:
          callable: |
            import re
            def all_words(row):
              return re.findall(r'[a-z]+', row.line.lower())
        value: 1
    input: Read from GCS
  - type: Explode
    name: Explode word arrays
    config:
      fields: [word]
    input: Split words
  - type: Combine
    name: Count words
    config:
      group_by: [word]
      combine:
        value: sum
    input: Explode word arrays
  - type: MapToFields
    name: Format output
    config:
      language: python
      fields:
        output: "word + ': ' + str(value)"
    input: Count words
  - type: WriteToText
    name: Write to GCS
    config:
      path: gs://bucket/counts.txt
    input: Format output

tests: []
```

To write tests for this pipeline, one creates a `tests` section that enumerates
a number of tests, each of which provide example input and assert the expected
output is produced.  An example test might be as follows

```
tests:
- name: MyRegressionTest
  mock_outputs:
    - name: Read from GCS
      elements:
        - line: "Nothing can come of nothing"
  expected_inputs:
    - name: Write to GCS
      elements:
        - output: 'nothing: 2'
        - output: 'can: 1'
        - output: 'come: 1'
        - output: 'of: 1'
```

The `mock_outputs` section designates that the transform named `Read from GCS`
should produce the single row `{line: "Nothing can come of nothing"}` for the
purposes of this test, and the `expected_inputs` section indicates that the
transform `Write to GCS` should expect to receive exactly the given elements.
Neither the actual Read transform nor Write transform from the original
pipelines are executed when running the test, but all intermediate transforms
are.

This test can then be executed by running

```
python -m apache_beam.yaml.main \
    --yaml_pipeline_file=wordcount.yaml \
    --tests
```

Alternatively, the a `tests:` block may be placed in a separate file and be
validated by running

```
python -m apache_beam.yaml.main \
    --yaml_pipeline_file=wordcount.yaml \
    --tests \
    --test_suite=test_file.yaml
```

For hermeticity, we require that all inputs (with the exception of
`Create`) that are needed to compute the expected outputs are explicitly mocked;
to explicitly allow a sources to be executed as part of a test their names or
types can be enumerated in an `allowed_sources` attribute of the test
specification.


## Pipeline fragment tests

One can also test a portion of a pipeline using the `mock_inputs` and
`expected_outputs` section of a test, for example

```
tests:
- name: TestSplittingWithPunctuation
  mock_inputs:
    - name: Split words
      elements:
        - line: "lots-of-words"
        - line: "...and more"
  expected_outputs:
    - name: Explode
      elements:
        - word: lots
          value: 1
        - word: of
          value: 1
        - word: words
          value: 1
        - word: and
          value: 1
        - word: more
          value: 1

- name: TestCombineAndFormat
  mock_inputs:
    - name: Count words
      elements:
        - word: more
          value: 1
        - word: and
          value: 1
        - word: more
          value: 1
  expected_outputs:
    - name: Format output
      elements:
        - output: "more: 2"
        - output: "and: 1"
```

As before, each test only executes the portion of the pipeline between the
mock inputs and expected outputs.  Note that the named transform in a
`mock_inputs` specification *is* executed, while the named transform of a
`mock_outputs` specification is not.
Similarly, the named transform of a `expected_inputs` specification is *not*
executed, while the named transform of an `expected_outputs` necessarily is.


## Automatically generating tests.

In an effort to make tests as easy to write and maintain as possible,
Beam YAML provides utilities to compute the expected outputs for your tests.


Running

```
python -m apache_beam.yaml.main \
    --yaml_pipeline_file=wordcount.yaml \
    --tests \
    [--test_suite=...] \
    --create_test
```

will create an entirely new test by sampling all the sources and
constructing a test accordingly.

One can also keep tests up to date by running

```
python -m apache_beam.yaml.main \
    --yaml_pipeline_file=wordcount.yaml \
    --tests \
    [--test_suite=...] \
    --fix_tests
```

which will update any existing `expected_input` and `expected_output` blocks
of your pipeline to contain the actual values computed during the test.
This can be useful in authoring tests as well--one can simply specify a
nonsensical or empty elements block in the expectation and the `--fix_tests`
flag will populate it for you.
(Of course, it is on any user of these flags to verify that the produced values
are meaningful and as expected.)


## Branching pipelines

For complex, branching pipelines, any number of `mock_inputs` and `mock_outputs`
may be enumerated to provide the input data, and any number of `expected_inputs`
and `expected_outputs` validations may be specified as well.
In both the `mock_outputs` and `expected_outputs` block, multiple outputs can
be disambiguated with the `TransformName.output_name` notation just as when
authoring a yaml pipeline.

```
pipeline:
  transforms:
    - type: Create
      name: Abc
      config:
        elements: [a, b, ccc]
    - type: Create
      name: Xyz
      config:
        elements: [x, y, zzz]
    - type: MapToFields
      name: Upper
      input: [Abc, Xyz]
      config:
        language: python
        fields:
          element: element.upper()
    - type: Partition
      input: Upper
      config:
        language: python
        by: '"big" if len(element) > 1 else "small"'
        outputs: ["big", "small"]
    - type: MapToFields
      name: MaybeHasErrors
      input: Abc
      config:
        language: python
        fields:
          inverse_size: 1 / len(element)
        error_handling:
          output: errors
    - type: StripErrorMetadata
      input: MaybeHasErrors.errors

tests:
  - name: MockMultipleInputs
    mock_outputs:
      - name: Abc
        elements: [element: a]
      - name: Xyz
        elements: [element: z]
    expected_outputs:
      - name: Upper
        elements: [element: A, element: Z]

  - name: TestMultipelOuptuts
    mock_inputs:
      - name: Upper
        elements: [element: m, element: nnn]
    expected_outputs:
      - name: Partition.big
        elements: [element: NNN]
      - name: Partition.small
        elements: [element: M]

  - name: TestErrorHandling
    mock_outputs:
      - name: Abc
        elements: [element: 'Aaaa', element: '']
    expected_outputs:
      - name: MaybeHasErrors
        elements: [inverse_size: 0.25]
      - name: StripErrorMetadata
        elements: [element: '']
```
