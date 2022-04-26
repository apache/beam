---
title: "Learn about Beam"
aliases:
  - /learn/
  - /docs/learn/
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

# Apache Beam Documentation

This page provides links to conceptual information and reference material for
the Beam programming model, SDKs, and runners.

## Concepts

Learn about the Beam Programming Model and the concepts common to all Beam SDKs
and Runners.

* Start with the [Basics of the Beam model](/documentation/basics/) for
  introductory conceptual information.
* Read the [Programming Guide](/documentation/programming-guide/), which
  has more detailed information about the Beam concepts and provides code
  snippets.
* Learn about Beam's [execution model](/documentation/runtime/model) to better
  understand how pipelines execute.
* Visit [Learning Resources](/documentation/resources/learning-resources) for
  some of our favorite articles and talks about Beam.
* Reference the [glossary](/documentation/glossary) to learn the terminology of the
  Beam programming model.

## Pipeline Fundamentals

* [Design Your Pipeline](/documentation/pipelines/design-your-pipeline/) by
  planning your pipeline’s structure, choosing transforms to apply to your data,
  and determining your input and output methods.
* [Create Your Pipeline](/documentation/pipelines/create-your-pipeline/) using
  the classes in the Beam SDKs.
* [Test Your Pipeline](/documentation/pipelines/test-your-pipeline/) to minimize
  debugging a pipeline’s remote execution.

## SDKs

Find status and reference information on all of the available Beam SDKs.

{{< documentation/sdks >}}

## Transform catalogs

Beam's transform catalogs contain explanations and code snippets for Beam's
built-in transforms.

 * [Java transform catalog](/documentation/transforms/java/overview/)
 * [Python transform catalog](/documentation/transforms/python/overview/)

## Runners

A Beam Runner runs a Beam pipeline on a specific (often distributed) data
processing system.

### Available Runners

{{< documentation/runners >}}

### Choosing a Runner

Beam is designed to enable pipelines to be portable across different runners.
However, given every runner has different capabilities, they also have different
abilities to implement the core concepts in the Beam model. The
[Capability Matrix](/documentation/runners/capability-matrix/) provides a
detailed comparison of runner functionality.

Once you have chosen which runner to use, see that runner's page for more
information about any initial runner-specific setup as well as any required or
optional `PipelineOptions` for configuring its execution. You might also want to
refer back to the Quickstart for [Java](/get-started/quickstart-java),
[Python](/get-started/quickstart-py) or [Go](/get-started/quickstart-go) for
instructions on executing the sample WordCount pipeline.

