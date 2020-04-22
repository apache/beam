---
layout: section
title: "Apache Beam Capability Matrix"
section_menu: section-menu/runners.html
permalink: /documentation/runners/capability-matrix/
redirect_from:
  - /learn/runners/capability-matrix/
  - /capability-matrix/
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

# Beam Capability Matrix
Apache Beam provides a portable API layer for building sophisticated data-parallel processing pipelines that may be executed across a diversity of execution engines, or <i>runners</i>. The core concepts of this layer are based upon the Beam Model (formerly referred to as the [Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)), and implemented to varying degrees in each Beam runner. To help clarify the capabilities of individual runners, we've created the capability matrix below.

Individual capabilities have been grouped by their corresponding <span class="wwwh-what-dark">What</span> / <span class="wwwh-where-dark">Where</span> / <span class="wwwh-when-dark">When</span> / <span class="wwwh-how-dark">How</span> question:

- <span class="wwwh-what-dark">What</span> results are being calculated?
- <span class="wwwh-where-dark">Where</span> in event time?
- <span class="wwwh-when-dark">When</span> in processing time?
- <span class="wwwh-how-dark">How</span> do refinements of results relate?

For more details on the <span class="wwwh-what-dark">What</span> / <span class="wwwh-where-dark">Where</span> / <span class="wwwh-when-dark">When</span> / <span class="wwwh-how-dark">How</span> breakdown of concepts, we recommend reading through the <a href="https://oreilly.com/ideas/the-world-beyond-batch-streaming-102">Streaming 102</a> post on O'Reilly Radar.

Note that in the future, we intend to add additional tables beyond the current set, for things like runtime characterstics (e.g. at-least-once vs exactly-once), performance, etc.

{% include capability-matrix-common.md %}
{% assign cap-data=site.data.capability-matrix %}

<center>

<!-- Summary table -->
{% assign cap-style='cap-summary' %}
{% assign cap-view='summary' %}
{% assign cap-other-view='full' %}
{% assign cap-toggle-details=1 %}
{% assign cap-display='block' %}

{% include capability-matrix.md %}

<!-- Full details table -->
{% assign cap-style='cap' %}
{% assign cap-view='full' %}
{% assign cap-other-view='summary' %}
{% assign cap-toggle-details=0 %}
{% assign cap-display='none' %}

{% include capability-matrix.md %}
</center>
