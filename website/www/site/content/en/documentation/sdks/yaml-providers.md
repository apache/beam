---
type: languages
title: "Apache Beam YAML Providers"
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

# Providers

Though we aim to offer a large suite of built-in transforms, it is inevitable
that people will need to author their own. This is made possible
through the notion of Providers which leverage expansion services and
vend catalogues of schema transforms.

## Java

Exposing transform in Java that can be used in a YAML pipeline consists of
four main steps:

1. Defining the transformation itself as a
   [PTransform](https://beam.apache.org/documentation/programming-guide/#composite-transforms)
   that consumes and produces zero or more [schema'd PCollections](https://beam.apache.org/documentation/programming-guide/#creating-schemas).
2. Exposing this transform via a
   [SchemaTransformProvider](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
   which provides an identifier used to refer to this transform later as well
   as metadata like a human-readable description and its configuration parameters.
3. Building a Jar that contains these classes and vends them via the
   [Service Loader](https://github.com/apache/beam-starter-java-provider/blob/main/src/main/java/org/example/ToUpperCaseTransformProvider.java#L47)
   infrastructure.
4. Writing a [provider specification](https://beam.apache.org/documentation/sdks/yaml/#providers)
   that tells Beam YAML where to find this jar and what it contains.

If the transform is already exposed as a
[cross language transform](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines/)
or [schema transform](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/schemas/transforms/SchemaTransformProvider.html)
then steps 1-3 have been done for you.  One then uses this transform as follows:

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

We provide a
[full cloneable example of how to build a java provider](https://github.com/apache/beam-starter-java-provider)
that can be used to get started.

## Python

Arbitrary Python transforms can be provided as well, using the syntax

```
providers:
  - type: pythonPackage
    config:
      packages:
        - my_pypi_package>=version
        - /path/to/local/package.zip
    transforms:
      MyCustomTransform: "pkg.module.PTransformClassOrCallable"
```

which can then be used as

```
- type: MyCustomTransform
  config:
    num: 3
    arg: whatever
```

This will cause the dependencies to be installed before the transform is
imported (via its given fully qualified name) and instantiated
with the config values passed as keyword arguments (e.g. in this case
`pkg.module.PTransformClassOrCallable(num=3, arg="whatever")`).

We offer a [python provider starter project](https://github.com/apache/beam-starter-python-provider)
that serves as a complete example for how to do this.

## YAML

New, re-usable transforms can be defined in YAML as well.
This type of provider simply has a mapping of names to their YAML definitions.
Jinja2 templatization of their string representations is used to parameterize
them.

The `config_schema` section of the transform definition specifies what
parameters are required (with their types) and the `body` section gives
the implementation in terms of other YAML transforms.

```
- type: yaml
  transforms:
    # Define the first transform of type "RaiseElementToPower"
    RaiseElementToPower:
      config_schema:
        properties:
          n: {type: integer}
      body:
        type: chain
        transforms:
          - type: MapToFields
            config:
              language: python
              append: true
              fields:
                power: "element ** {{n}}"

    # Define a second transform that produces consecutive integers.
    Range:
      config_schema:
        properties:
          end: {type: integer}
      # Setting this parameter lets this transform type be used as a source.
      requires_inputs: false
      body: |
        type: Create
        config:
          elements:
            {% for ix in range(end) %}
            - {{ix}}
            {% endfor %}
```

Note that in this second example the `body` of Range is defined as a
[block string literal](https://yaml-multiline.info/)
to prevent any attempt by the system to parse the `{%` and `%}` pragmas used
for control statements before a specialization with a concrete value for `end`
is instantiated and the loop is expanded.

These could then be used in a pipeline as

```
transforms:
  - type: Range
    config:
      end: 10
  - type: RaiseElementToPower
    input: Range
    config:
      n: 3
  ...
```

One can define composite transforms as well, e.g. in a provider listing one
could have

```
- type: yaml
  transforms:
    ConsecutivePowers:
      # This takes two parameters.
      config_schema:
        properties:
          end: {type: integer}
          n: {type: integer}

      # It can be used as a source transform.
      requires_inputs: false

      # The body uses the transforms defined above linked together in a chain.
      body: |
        type: chain
        transforms:
          - type: Range
            config:
              end: {{end}}
          - type: RaiseElementToPower
            config:
              n: {{n}}
```

which allows one to use this whole fragment as

```
type: ConsecutivePowers
config:
  end: 10
  n: 3
```

Note that YAML-defined transforms work better in a listing file than directly
in the `providers` block of a pipeline file as pipeline files are always
pre-processed with Jinja2 themselves which would necessitate double escaping.

## YAML Provider listing files

One can reference an external listings of providers in the yaml pipeline file
via the syntax

```
providers:
  - include: "file:///path/to/local/providers.yaml"
  - include: "gs://path/to/remote/providers.yaml"
  - include: "https://example.com/hosted/providers.yaml"
  ...
```

where `providers.yaml` is simply a yaml file containing a list of providers
in the same format as those inlined in this providers block.
See, for example, the provider listing [here](
https://github.com/apache/beam-starter-python-provider/blob/main/examples/provider_listing.yaml).

In fact, this is how many of the the built in transforms are declared,
see for example the [builtin io listing file](
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/standard_io.yaml).

Hosting these listing files (together with their required artifacts) allows
one to easily share catalogues of transforms that can be directly used
by others in their YAML pipelines.
