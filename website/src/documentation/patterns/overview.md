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
* [Processing files as they arrive]({{ site.baseurl }}/documentation/patterns/file-processing/#processing-files-as-they-arrive)
* [Accessing filenames]({{ site.baseurl }}/documentation/patterns/file-processing/#accessing-filenames)

**Side input patterns** - Patterns for processing supplementary data
* [Slowly updating global window side inputs]({{ site.baseurl }}/documentation/patterns/side-inputs/#slowly-updating-global-window-side-inputs)

**Pipeline option patterns** - Patterns for configuring pipelines
* [Retroactively logging runtime parameters]({{ site.baseurl }}/documentation/patterns/pipeline-options/#retroactively-logging-runtime-parameters)

**Custom I/O patterns** - Patterns for pipeline I/O
* [Choosing between built-in and custom connectors]({{ site.baseurl }}/documentation/patterns/custom-io/#choosing-between-built-in-and-custom-connectors)

**Custom window patterns** - Patterns for windowing functions
* [Using data to dynamically set session window gaps]({{ site.baseurl }}/documentation/patterns/custom-windows/#using-data-to-dynamically-set-session-window-gaps)

## Contributing a pattern

To contribute a new pipeline pattern, create an issue with the [`pipeline-patterns` label](https://issues.apache.org/jira/browse/BEAM-7449?jql=labels%20%3D%20pipeline-patterns) and add details to the issue description. See [Get started contributing]({{ site.baseurl }}/contribute/) for more information.

## What's next

* Try an [end-to-end example]({{ site.baseurl }}/get-started/try-apache-beam/)
* Execute your pipeline on a [runner]({{ site.baseurl }}/documentation/runners/capability-matrix/)
