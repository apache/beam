---
type: languages
title: "Apache Beam YAML API"
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

# Beam YAML API

Beam YAML is a declarative syntax for describing Apache Beam pipelines by using
YAML files. You can use Beam YAML to author and run a Beam pipeline without
writing any code.

## Overview

Beam provides a powerful model for creating sophisticated data processing
pipelines. However, getting started with Beam programming can be challenging
because it requires writing code in one of the supported Beam SDK languages.
You need to understand the APIs, set up a project, manage dependencies, and
perform other programming tasks.

Beam YAML makes it easier to get started with creating Beam pipelines. Instead
of writing code, you create a YAML file using any text editor. Then you submit
the YAML file to be executed by a runner.

The Beam YAML syntax is designed to be human-readable but also suitable as an
intermediate representation for tools. For example, a pipeline authoring GUI
could output YAML, or a lineage analysis tool could consume the YAML pipeline
specifications.

Beam YAML is still under development, but any features already included are
considered stable. Feedback is welcome at dev@apache.beam.org.

## Prerequisites

The Beam YAML parser is currently included as part of the
[Apache Beam Python SDK](../python/). You don't need to write Python code to use
Beam YAML, but you need the SDK to run pipelines locally.

We recommend creating a
[virtual environment](../../../get-started/quickstart/python/#create-and-activate-a-virtual-environment)
so that all packages are installed in an isolated and self-contained
environment. After you set up your Python environment, install the SDK as
follows:

```
pip install apache_beam[yaml,gcp]
```

In addition, several of the provided transforms, such as the SQL transform, are
implemented in Java and require a working Java interpeter. When you a run a
pipeline with these transforms, the required artifacts are automatically
downloaded from the Apache Maven repositories.

## Getting started

Use a text editor to create a file named `pipeline.yaml`. Paste the following
text into the file and save:

```yaml
pipeline:
  transforms:
    - type: Create
      config:
        elements: [1, 2, 3]
    - type: LogForTesting
      input: Create
```

This file defines a simple pipeline with two transforms:

- The `Create` transform creates a collection. The value of `config` is a
  dictionary of configuration settings. In this case, `elements` specifies the
  members of the collection. Other transform types have other configuration
  settings.
- The `LogForTesting` transform logs each input element. This transform doesn't
  require a `config` setting. The `input` key specifies that `LogForTesting`
  receives input from the `Create` transform.

### Run the pipeline

To execute the pipeline, run the following Python command:

```sh
python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml
```

The output should contain log statements similar to the following:

```sh
INFO:root:{"element": 1}
INFO:root:{"element": 2}
INFO:root:{"element": 3}
```

### Run the pipeline in Dataflow

You can submit a YAML pipeline to Dataflow by using the
[gcloud CLI](https://cloud.google.com/sdk/gcloud). To create a Dataflow job
from the YAML file, use the
[`gcloud dataflow yaml run`](https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml/run)
command:

```
gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file=pipeline.yaml \
  --region=$REGION
```

When you use the `gcloud` CLI, you don't need to install the Beam SDKs locally.


### Visualize the pipeline

You can use the
[`apache_beam.runners.render`](https://beam.apache.org/releases/pydoc/current/apache_beam.runners.render.html)
module to render the pipeline execution graph as a PNG file, as follows:

1. Install [Graphviz](https://graphviz.org/download/).
1. Run the following command:

   ```
   python -m apache_beam.yaml.main --yaml_pipeline_file=pipeline.yaml \
     --runner=apache_beam.runners.render.RenderRunner \
     --render_output=out.png
   ```

## Example: Reading CSV data

The following pipeline reads data from a set of CSV files and writes the data in
JSON format. This pipeline assumes the CSV files have a header row. The column
names become JSON field names.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv
    - type: WriteToJson
      config:
        path: /path/to/output.json
      input: ReadFromCsv
```

### Add a filter

The [`Filter`](../yaml-udf/#filtering) transform filters records. It keeps input
records that satisfy a Boolean predicate and discards records that don't
satisify the predicate. The following example keeps records where the value of
`col3` is greater than 100:

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv
    - type: Filter
      config:
        language: python
        keep: "col3 > 100"
      input: ReadFromCsv
    - type: WriteToJson
      config:
        path: /path/to/output.json
      input: Filter
```

### Add a mapping function

Beam YAML supports various [mapping functions](../yaml-udf/#mapping-functions).
The following example uses the `Sql` transform to group by `col1` and output the
counts for each key.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv
    - type: Filter
      config:
        language: python
        keep: "col3 > 100"
      input: ReadFromCsv
    - type: Sql
      config:
        query: "select col1, count(*) as cnt from PCOLLECTION group by col1"
      input: Filter
    - type: WriteToJson
      config:
        path: /path/to/output.json
      input: Sql
```

## Patterns

This section describes some common patterns in Beam YAML.

### Named transforms

You can name the transforms in your pipeline to help with monitoring and
debugging. Names are also used to disambiguate transforms if the pipeline
contains more than one transform of the same type.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      name: ReadMyData
      config:
        path: /path/to/input*.csv
    - type: Filter
      name: KeepBigRecords
      config:
        language: python
        keep: "col3 > 100"
      input: ReadMyData
    - type: Sql
      name: MySqlTransform
      config:
        query: "select col1, count(*) as cnt from PCOLLECTION group by col1"
      input: KeepBigRecords
    - type: WriteToJson
      name: WriteTheOutput
      config:
        path: /path/to/output.json
      input: MySqlTransform
```

### Chaining transforms

If a pipeline is linear (no branching or merging), you can designate the
pipeline as a `chain` type. In a `chain`-type pipeline, you don't need to
specify the inputs. The inputs are implicit from the order they appear in the
YAML file:

```
pipeline:
  type: chain

  transforms:
    - type: ReadFromCsv
      config:
        path: /path/to/input*.csv
    - type: Filter
      config:
        language: python
        keep: "col3 > 100"
    - type: Sql
      name: MySqlTransform
      config:
        query: "select col1, count(*) as cnt from PCOLLECTION group by col1"
    - type: WriteToJson
      config:
        path: /path/to/output.json
```

### Source and sink transforms

As syntactic sugar, you can name the first and last transforms in your pipeline
as `source` and `sink`. This convention does not change the resulting pipeline,
but it signals the intent of the source and sink transforms.

```
pipeline:
  type: chain

  source:
    type: ReadFromCsv
    config:
      path: /path/to/input*.csv

  transforms:
    - type: Filter
      config:
        language: python
        keep: "col3 > 100"

    - type: Sql
      name: MySqlTransform
      config:
        query: "select col1, count(*) as cnt from PCOLLECTION group by col1"

  sink:
    type: WriteToJson
    config:
      path: /path/to/output.json
```

### Non-linear pipelines

Beam YAML supports arbitrary non-linear pipelines. The following pipeline reads
two sources, joins them, and writes two outputs:

```
pipeline:
  transforms:
    - type: ReadFromCsv
      name: ReadLeft
      config:
        path: /path/to/left*.csv

    - type: ReadFromCsv
      name: ReadRight
      config:
        path: /path/to/right*.csv

    - type: Sql
      config:
        query: select A.col1, B.col2 from A join B using (col3)
      input:
        A: ReadLeft
        B: ReadRight

    - type: WriteToJson
      name: WriteAll
      input: Sql
      config:
        path: /path/to/all.json

    - type: Filter
      name: FilterToBig
      input: Sql
      config:
        language: python
        keep: "col2 > 100"

    - type: WriteToCsv
      name: WriteBig
      input: FilterToBig
      config:
        path: /path/to/big.csv
```

Because the pipeline is not linear, you must explicitly declare the inputs for
each transform. However, you can nest a `chain` within a non-linear pipeline.
The chain is a linear sub-path within the pipeline.

The following example creates a chain named `ExtraProcessingForBigRows`. The
chain takes input from the `Sql` transform and applies several additional
filters plus a sink. Notice that within the chain, the inputs don't need to be
specified.

```
pipeline:
  transforms:
    - type: ReadFromCsv
      name: ReadLeft
      config:
        path: /path/to/left*.csv

    - type: ReadFromCsv
      name: ReadRight
      config:
        path: /path/to/right*.csv

    - type: Sql
      config:
        query: select A.col1, B.col2 from A join B using (col3)
      input:
        A: ReadLeft
        B: ReadRight

    - type: WriteToJson
      name: WriteAll
      input: Sql
      config:
        path: /path/to/all.json

    - type: chain
      name: ExtraProcessingForBigRows
      input: Sql
      transforms:
        - type: Filter
          config:
            language: python
            keep: "col2 > 100"
        - type: Filter
          config:
            language: python
            keep: "len(col1) > 10"
        - type: Filter
          config:
            language: python
            keep: "col1 > 'z'"
      sink:
        type: WriteToCsv
        config:
          path: /path/to/big.csv
```

## Windowing

This API can be used to define both streaming and batch pipelines.
In order to meaningfully aggregate elements in a streaming pipeline,
some kind of windowing is typically required. Beam's
[windowing](https://beam.apache.org/documentation/programming-guide/#windowing)
and [triggering](https://beam.apache.org/documentation/programming-guide/#triggers)
can be declared using the same `WindowInto` transform available in all other
Beam SDKs.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: JSON
        schema:
          type: object
          properties:
            col1: {type: string}
            col2: {type: integer}
            col3: {type: number}
    - type: WindowInto
      windowing:
        type: fixed
        size: 60s
    - type: SomeGroupingTransform
      config:
        arg: ...
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
options:
  streaming: true
```

Rather than using an explicit `WindowInto` operation, you can tag a transform
with a specified windowing, which causes its inputs (and hence the transform
itself) to be applied with that windowing.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: ...
        schema: ...
    - type: SomeGroupingTransform
      config:
        arg: ...
      windowing:
        type: sliding
        size: 60s
        period: 10s
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
options:
  streaming: true
```

Note that the `Sql` operation itself is often a from of aggregation, and
applying a windowing (or consuming an already windowed input) causes all
grouping to be done per window.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: ...
        schema: ...
    - type: Sql
      config:
        query: "select col1, count(*) as c from PCOLLECTION"
      windowing:
        type: sessions
        gap: 60s
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
options:
  streaming: true
```

The specified windowing is applied to all inputs, in this case resulting in
a join per window.

```
pipeline:
  transforms:
    - type: ReadFromPubSub
      name: ReadLeft
      config:
        topic: leftTopic
        format: ...
        schema: ...

    - type: ReadFromPubSub
      name: ReadRight
      config:
        topic: rightTopic
        format: ...
        schema: ...

    - type: Sql
      config:
        query: select A.col1, B.col2 from A join B using (col3)
      input:
        A: ReadLeft
        B: ReadRight
      windowing:
        type: fixed
        size: 60s
options:
  streaming: true
```

For a transform with no inputs, the specified windowing is instead applied to
its output(s). As per the Beam model, the windowing is then inherited by all
consuming operations. This is especially useful for root operations like Read.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: ...
        schema: ...
      windowing:
        type: fixed
        size: 60s
    - type: Sql
      config:
        query: "select col1, count(*) as c from PCOLLECTION"
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
options:
  streaming: true
```

One can also specify windowing at the top level of a pipeline (or composite),
which is a shorthand for applying this same windowing to all root
operations that don't otherwise specify their own windowing. This approach is
effective way to apply a window everywhere in the pipeline.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: ...
        schema: ...
    - type: Sql
      config:
        query: "select col1, count(*) as c from PCOLLECTION"
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
  windowing:
    type: fixed
    size: 60
options:
  streaming: true
```

Note that all these windowing specifications are compatible with the `source`
and `sink` syntax as well:

```
pipeline:
  type: chain

  source:
    type: ReadFromPubSub
    config:
      topic: myPubSubTopic
      format: ...
      schema: ...
    windowing:
      type: fixed
      size: 10s

  transforms:
    - type: Sql
      config:
        query: "select col1, count(*) as c from PCOLLECTION"

  sink:
    type: WriteToCsv
    config:
      path: /path/to/output.json
    windowing:
      type: fixed
      size: 5m

options:
  streaming: true
```


## Providers

Though we aim to offer a large suite of built-in transforms, it is inevitable
that people will want to author their own. This is made possible
through the notion of Providers which leverage expansion services and
schema transforms.

For example, you could build a jar that vends a
[cross language transform](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines/)
or [schema transform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
and then use it in a transform as follows

```
pipeline:
  type: chain
  source:
    type: ReadFromCsv
    config:
      path: /path/to/input*.csv

  transforms:
    - type: MyCustomTransform
      config:
        arg: whatever

  sink:
    type: WriteToJson
    config:
      path: /path/to/output.json

providers:
  - type: javaJar
    config:
       jar: /path/or/url/to/myExpansionService.jar
    transforms:
       MyCustomTransform: "urn:registered:in:expansion:service"
```

Arbitrary Python transforms can be provided as well, using the syntax

```
providers:
  - type: pythonPackage
    config:
       packages:
           - my_pypi_package>=version
           - /path/to/local/package.zip
    transforms:
       MyCustomTransform: "pkg.subpkg.PTransformClassOrCallable"
```

## Pipeline options

[Pipeline options](https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options)
are used to configure different aspects of your pipeline, such as the pipeline runner that will execute
your pipeline and any runner-specific configuration required by the chosen runner. To set pipeline options,
append an options block at the end of your yaml file. For example:

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: ...
        schema: ...
      windowing:
        type: fixed
        size: 60s
    - type: Sql
      config:
        query: "select col1, count(*) as c from PCOLLECTION"
    - type: WriteToPubSub
      config:
        topic: anotherPubSubTopic
        format: JSON
options:
  streaming: true
```

## Other Resources

* [Example pipeline](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples)
* [More examples](https://gist.github.com/robertwb/2cb26973f1b1203e8f5f8f88c5764da0)
