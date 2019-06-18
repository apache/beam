---
layout: section
title: "Overview"
section_menu: section-menu/documentation.html
permalink: /documentation/patterns/overview/
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

# Common pipeline patterns

Pipeline patterns demonstrate common Beam use cases. Pipeline patterns are based on real-world Beam deployments. Each pattern has a description, examples, and a solution or psuedocode.

**File processing patterns** - Patterns for reading from and writing to files
* [Processing files as they arrive]({{ site.baseurl }}/documentation/patterns/file-processing-patterns/#processing-files-as-they-arrive)
* [Accessing filenames]({{ site.baseurl }}/documentation/patterns/file-processing-patterns/#accessing-filenames)

**Side input patterns** - Patterns for processing supplementary data
* [Using global window side inputs in non-global windows]({{ site.baseurl }}/documentation/patterns/side-input-patterns/#using-global-window-side-inputs-in-non-global-windows)

**Pipeline option patterns** - Patterns for configuring pipelines
* [Retroactively logging runtime parameters]({{ site.baseurl }}/documentation/patterns/pipeline-option-patterns/#retroactively-logging-runtime-parameters)

**Custom I/O patterns**
* [Choosing between built-in and custom connectors]({{ site.baseurl }}/documentation/patterns/custom-io-patterns/#choosing-between-built-in-and-custom-connectors)

## Contributing a pattern

To contribute a new pipeline pattern, create an issue with the [`pipeline-patterns` label](https://issues.apache.org/jira/browse/BEAM-7449?jql=labels%20%3D%20pipeline-patterns) and add details to the issue description. See [Get started contributing]({{ site.baseurl }}/contribute/) for more information.

## What's next

* Try an [end-to-end example]({{ site.baseurl }}/get-started/try-apache-beam/)
* Execute your pipeline on a [runner]({{ site.baseurl }}/documentation/runners/capability-matrix/)