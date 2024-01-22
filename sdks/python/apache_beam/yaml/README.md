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

While Beam provides powerful APIs for authoring sophisticated data
processing pipelines, it often still has too high a barrier for
getting started and authoring simple pipelines. Even setting up the
environment, installing the dependencies, and setting up the project
can be an overwhelming amount of boilerplate for some (though
https://beam.apache.org/blog/beam-starter-projects/ has gone a long
way in making this easier).

Here we provide a simple declarative syntax for describing pipelines
that does not require coding experience or learning how to use an
SDK&mdash;any text editor will do.
Some installation may be required to actually *execute* a pipeline, but
we envision various services (such as Dataflow) to accept yaml pipelines
directly obviating the need for even that in the future.
We also anticipate the ability to generate code directly from these
higher-level yaml descriptions, should one want to graduate to a full
Beam SDK (and possibly the other direction as well as far as possible).

Though we intend this syntax to be easily authored (and read) directly by
humans, this may also prove a useful intermediate representation for
tools to use as well, either as output (e.g. a pipeline authoring GUI)
or consumption (e.g. a lineage analysis tool) and expect it to be more
easily manipulated and semantically meaningful than the Beam protos
themselves (which concern themselves more with execution).

It should be noted that everything here is still under development, but any
features already included are considered stable. Feedback is welcome at
dev@apache.beam.org.

## Running pipelines

The Beam yaml parser is currently included as part of the Apache Beam Python SDK.
This can be installed (e.g. within a virtual environment) as

```
pip install apache_beam[yaml,gcp]
```

In addition, several of the provided transforms (such as SQL) are implemented
in Java and their expansion will require a working Java interpeter. (The
requisite artifacts will be automatically downloaded from the apache maven
repositories, so no further installs will be required.)
Docker is also currently required for local execution of these
cross-language-requiring transforms, but not for submission to a non-local
runner such as Flink or Dataflow.

Once the prerequisites are installed, you can execute a pipeline defined
in a yaml file as

```
python -m apache_beam.yaml.main --yaml_pipeline_file=/path/to/pipeline.yaml [other pipeline options such as the runner]
```

You can do a dry-run of your pipeline using the render runner to see what the
execution graph is, e.g.

```
python -m apache_beam.yaml.main --yaml_pipeline_file=/path/to/pipeline.yaml --runner=apache_beam.runners.render.RenderRunner --render_output=out.png [--render_port=0]
```

(This requires [Graphviz](https://graphviz.org/download/) to be installed to render the pipeline.)

We intend to support running a pipeline on Dataflow by directly passing the
yaml specification to a template, no local installation of the Beam SDKs required.

## Example pipelines

Here is a simple pipeline that reads some data from csv files and
writes it out in json format.

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

We can also add a transformation

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

or two.

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

Transforms can be named to help with monitoring and debugging.

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

(This is also needed to disambiguate if more than one transform of the same
type is used.)

If the pipeline is linear, we can let the inputs be implicit by designating
the pipeline as a `chain` type.

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

As syntactic sugar, we can name the first and last transforms in our pipeline
as `source` and `sink`.

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

Arbitrary non-linear pipelines are supported as well, though in this case
inputs must be explicitly named.
Here we read two sources, join them, and write two outputs.

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

One can, however, nest `chains` within a non-linear pipeline.
For example, here `ExtraProcessingForBigRows` is itself a "chain" transform
that has a single input and contains its own sink.

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
can be declared using the same WindowInto transform available in all other
SDKs.

```
pipeline:
  type: chain
  transforms:
    - type: ReadFromPubSub
      config:
        topic: myPubSubTopic
        format: json
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
        format: json
```

Rather than using an explicit `WindowInto` operation, one may instead tag a
transform itself with a specified windowing which will cause its inputs
(and hence the transform itself) to be applied with that windowing.

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
        format: json
```

Note that the `Sql` operation itself is often a from of aggregation, and
applying a windowing (or consuming an already windowed input) will cause all
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
        format: json
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
        format: json
```

One can also specify windowing at the top level of a pipeline (or composite),
which is a shorthand to simply applying this same windowing to all root
operations (that don't otherwise specify their own windowing),
and can be an effective way to apply it everywhere.

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
        format: json
  windowing:
    type: fixed
    size: 60
```

Note that all these windowing specifications are compatible with the `source`
and `sink` syntax as well

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
```


## Providers

Though we aim to offer a large suite of built-in transforms, it is inevitable
that people will want to be able to author their own. This is made possible
through the notion of Providers which leverage expansion services and
schema transforms.

For example, one could build a jar that vends a
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
    jar: /path/or/url/to/myExpansionService.jar
    transforms:
       MyCustomTransform: "urn:registered:in:expansion:service"
```

Arbitrary Python transforms can be provided as well, using the syntax

```
providers:
  - type: pythonPackage
    packages:
        - my_pypi_package>=version
        - /path/to/local/package.zip
    transforms:
       MyCustomTransform: "pkg.subpkg.PTransformClassOrCallable"
```

## Other Resources

* [Example pipelines](https://gist.github.com/robertwb/2cb26973f1b1203e8f5f8f88c5764da0)
* [More examples](https://github.com/Polber/beam/tree/jkinard/bug-bash/sdks/python/apache_beam/yaml/examples)
* [Transform glossary](https://gist.github.com/robertwb/64e2f51ff88320eeb6ffd96634202df7)

Additional documentation in this directory

* [Mapping](yaml_mapping.md)
* [Aggregation](yaml_combine.md)
* [Error handling](yaml_errors.md)
* [Inlining Python](inline_python.md)
