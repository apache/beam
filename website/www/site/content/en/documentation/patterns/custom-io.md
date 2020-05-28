---
title: "Custom I/O patterns"
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

# Custom I/O patterns

This page describes common patterns in pipelines with [custom I/O connectors](/documentation/io/developing-io-overview/). Custom I/O connectors connect pipelines to databases that aren't supported by Beam's [built-in I/O transforms](/documentation/io/built-in/).

{{< language-switcher java py >}}

## Choosing between built-in and custom connectors

[Built-in I/O connectors](/documentation/io/built-in/) are tested and hardened, so use them whenever possible. Only use custom I/O connectors when:

* No built-in options exist
* Your pipeline pulls in a small subset of source data

For instance, use a custom I/O connector to enrich pipeline elements with a small subset of source data. If you’re processing a sales order and adding information to each purchase, you can use a custom I/O connector to pull the small subset of data into your pipeline (instead of processing the entire source).

Beam distributes work across many threads, so custom I/O connectors can increase your data source’s load average. You can reduce the load with the <span class="language-java">[start](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.StartBundle.html)</span><span class="language-py">[start](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highlight=bundle#apache_beam.transforms.core.DoFn.start_bundle)</span> and <span class="language-java">[finish](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.FinishBundle.html)</span><span class="language-py">[finish](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highlight=bundle#apache_beam.transforms.core.DoFn.finish_bundle)</span> bundle annotations.