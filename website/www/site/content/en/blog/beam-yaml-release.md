---
title:  "Introducing Beam YAML: Apache Beam's First No-code SDK"
date:   2024-04-11 10:00:00 -0400
categories:
  - blog
authors:
  - jkinard

---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Writing a Beam pipeline can be a daunting task. Learning the Beam model, downloading dependencies for the SDK language
of choice, debugging the pipeline, and maintaining the pipeline code is a lot of overhead for users who want to write a
simple to intermediate data processing pipeline. There have been strides in making the SDK's entry points easier, but
for many, it is still a long way from being a painless process.

To address some of these issues and simplify the entry point to Beam, we have introduced a new way to specify Beam
pipelines by using configuration files rather than code. This new SDK, known as
[Beam YAML](https://beam.apache.org/documentation/sdks/yaml/), employs a declarative approach to creating
data processing pipelines using [YAML](https://yaml.org/), a widely used data serialization language.

<!--more-->

# Benefits of using Beam YAML

The primary goal of Beam YAML is to make the entry point to Beam as welcoming as possible. However, this should not
come at the expense of sacrificing the rich features that Beam offers.

Here are some of the benefits of using Beam YAML:

*   **No-code development:** Allows users to develop pipelines without writing any code. This makes it easier to get
    started with Beam and to develop pipelines quickly and easily.
*   **Maintainability**: Configuration-based pipelines are easier to maintain than code-based pipelines. YAML format
    enables clear separation of concerns, simplifying changes and updates without affecting other code sections.
*   **Declarative language:** Provides a declarative language, which means that it is based on the description of the
    desired outcome rather than expressing the intent through code. This makes it easy to understand the structure and
    flow of a pipeline. The YAML syntax is also widely used with a rich community of resources for learning and
    leveraging the YAML syntax.
*   **Powerful features:** Supports a wide range of features, including a variety of data sources and sinks, turn-key
    transforms, and execution parameters. This makes it possible to develop complex data processing pipelines with Beam
    YAML.
*   **Reusability**: Beam YAML promotes code reuse by providing a way to define and share common pipeline patterns. You
    can create reusable YAML snippets or blocks that can be easily shared and reused in different pipelines. This reduces
    the need to write repetitive tasks and helps maintain consistency across pipelines.
*   **Extensibility**: Beam YAML offers a structure for integrating custom transformations into a pipeline, enabling
    organizations to contribute or leverage a pre-existing catalog of transformations that can be seamlessly accessed
    using the Beam YAML syntax across multiple pipelines. It is also possible to build third-party extensions, including
    custom parsers and other tools, that do not need to depend on Beam directly.
*   **Backwards Compatibility**: Beam YAML is still being actively worked on, bringing exciting new features and
    capabilities, but as these features are added, backwards compatibility will be preserved. This way, once a pipeline
    is written, it will continue to work despite future released versions of the SDK.

Overall, using Beam YAML provides a number of advantages. It makes pipeline development and management more efficient
and effective, enabling users to focus on the business logic and data processing tasks, rather than spending time on
low-level coding details.


# Case Study: A simple business analytics use-case

Let's take the following sample transaction data for a department store:

| transaction_id  | product_name    | category     | price   |
|:----------------|:----------------|:-------------|:--------|
| T0012           | Headphones      | Electronics  | 59.99   |
| T5034           | Leather Jacket  | Apparel      | 109.99  |
| T0024           | Aluminum Mug    | Kitchen      | 29.99   |
| T0104           | Headphones      | Electronics  | 59.99   |
| T0302           | Monitor         | Electronics  | 249.99  |

Now, let's say that the business wants to get a record of transactions for all purchases made in the Electronics
department for audit purposes. Assuming the records are stored as a CSV file, a Beam YAML pipeline may look something
like this:

Source code for this example can be found
[here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/simple_filter.yaml).
```yaml
pipeline:
  transforms:
    - type: ReadFromCsv
      name: ReadInputFile
      config:
        path: /path/to/input.csv
    - type: Filter
      name: FilterWithCategory
      input: ReadInputFile
      config:
        language: python
        keep: category == "Electronics"
    - type: WriteToCsv
      name: WriteOutputFile
      input: FilterWithCategory
      config:
        path: /path/to/output
```

This would leave us with the following data:

| transaction_id  | product_name  | category     | price   |
|:----------------|:--------------|:-------------|:--------|
| T0012           | Headphones    | Electronics  | 59.99   |
| T0104           | Headphones    | Electronics  | 59.99   |
| T0302           | Monitor       | Electronics  | 249.99  |

Now, let's say the business wants to determine how much of each Electronics item is being sold to ensure that the
correct number is being ordered from the supplier. Let's also assume that they want to determine the total revenue for
each item. This simple aggregation can follow the Filter from the previous example as such:

Source code for this example can be found
[here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/simple_filter_and_combine.yaml).
```yaml
pipeline:
  transforms:
    - type: ReadFromCsv
      name: ReadInputFile
      config:
        path: /path/to/input.csv
    - type: Filter
      name: FilterWithCategory
      input: ReadInputFile
      config:
        language: python
        keep: category == "Electronics"
    - type: Combine
      name: CountNumberSold
      input: FilterWithCategory
      config:
        group_by: product_name
        combine:
          num_sold:
            value: product_name
            fn: count
          total_revenue:
            value: price
            fn: sum
    - type: WriteToCsv
      name: WriteOutputFile
      input: CountNumberSold
      config:
        path: /path/to/output
```

This would leave us with the following data:

| product_name  | num_sold  | total_revenue  |
|:--------------|:----------|:---------------|
| Headphones    | 2         | 119.98         |
| Monitor       | 1         | 249.99         |

While this was a relatively simple use-case, it shows the power of Beam YAML and how easy it is to go from business
use-case to a prototype data pipeline in just a few lines of YAML.


# Getting started with Beam YAML

There are several resources that have been compiled to help users get familiar with Beam YAML.


## Day Zero Notebook

<a target="_blank" href="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/get-started/try-apache-beam-yaml.ipynb">
<img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

To help get started with Apache Beam, there is a Day Zero Notebook available on
[Google Colab](https://colab.sandbox.google.com/), an online Python notebook environment with a free attachable
runtime, containing some basic YAML pipeline examples.


## Documentation

The Apache Beam website provides a set of [docs](https://beam.apache.org/documentation/sdks/yaml/) that demonstrate the
current capabilities of the Beam YAML SDK. There is also a catalog of currently-supported turnkey transforms found
[here](https://beam.apache.org/releases/yamldoc/current/).


## Examples

A catalog of examples can be found
[here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples). These examples showcase
all the turnkey transforms that can be utilized in Beam YAML. There are also a number of Dataflow Cookbook examples
that can be found [here](https://github.com/GoogleCloudPlatform/dataflow-cookbook/tree/main/Python/yaml).


## Contributing

Developers who wish to help build out and add functionalities are welcome to start contributing to the effort in the
Beam YAML module found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml).

There is also a list of open [bugs](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Ayaml) found
on the GitHub repo - now marked with the 'yaml' tag.

While Beam YAML has been marked stable as of Beam 2.52, it is still under heavy development, with new features being
added with each release. Those who wish to be part of the design decisions and give insights to how the framework is
being used are highly encouraged to join the dev mailing list as those discussions will be directed there. A link to
the dev list can be found [here](https://beam.apache.org/community/contact-us/).
