---
title: "Pipeline option patterns"
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

# Pipeline option patterns

The samples on this page show you common pipeline configurations. For more information about pipeline configuration options, see [Creating a pipeline](/documentation/programming-guide/#creating-a-pipeline) and [Configuring pipeline options](/documentation/programming-guide/#configuring-pipeline-options).

{{< language-switcher java py >}}

## Retroactively logging runtime parameters

Use the `ValueProvider` interface to access runtime parameters after completing a pipeline job.

You can use the `ValueProvider` interface to pass runtime parameters to your pipeline, but you can only log the parameters from within the the Beam DAG. A solution is to add a pipeline [branch](/documentation/programming-guide/#applying-transforms) with a `DoFn` that processes a placeholder value and then logs the runtime parameters:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" AccessingValueProviderInfoAfterRunSnip1 >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" AccessingValueProviderInfoAfterRunSnip1 >}}
{{< /highlight >}}
