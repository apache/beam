---
title:  "Clarifying & Formalizing Runner Capabilities"
date:   2016-03-17 11:00:00 -0700
categories:
  - beam
  - capability
aliases:
  - /beam/capability/2016/03/17/capability-matrix.html
authors:
  - fjp
  - takidau
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

With initial code drops complete ([Dataflow SDK and Runner](https://github.com/apache/beam/pull/1), [Flink Runner](https://github.com/apache/beam/pull/12), [Spark Runner](https://github.com/apache/beam/pull/42)) and expressed interest in runner implementations for [Storm](https://issues.apache.org/jira/browse/BEAM-9), [Hadoop](https://issues.apache.org/jira/browse/BEAM-19), and [Gearpump](https://issues.apache.org/jira/browse/BEAM-79) (amongst others), we wanted to start addressing a big question in the Apache Beam (incubating) community: what capabilities will each runner be able to support?

<!--more-->

While we’d love to have a world where all runners support the full suite of semantics included in the Beam Model (formerly referred to as the [Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)), practically speaking, there will always be certain features that some runners can’t provide. For example, a Hadoop-based runner would be inherently batch-based and may be unable to (easily) implement support for unbounded collections. However, that doesn’t prevent it from being extremely useful for a large set of uses. In other cases, the implementations provided by one runner may have slightly different semantics that those provided by another (e.g. even though the current suite of runners all support exactly-once delivery guarantees, an [Apache Samza](https://samza.apache.org/) runner, which would be a welcome addition, would currently only support at-least-once).

To help clarify things, we’ve been working on enumerating the key features of the Beam model in a [capability matrix](/documentation/runners/capability-matrix/) for all existing runners, categorized around the four key questions addressed by the model: <span class="wwwh-what-dark">What</span> / <span class="wwwh-where-dark">Where</span> / <span class="wwwh-when-dark">When</span> / <span class="wwwh-how-dark">How</span> (if you’re not familiar with those questions, you might want to read through [Streaming 102](https://oreilly.com/ideas/the-world-beyond-batch-streaming-102) for an overview). This table will be maintained over time as the model evolves, our understanding grows, and runners are created or features added.

Included below is a summary snapshot of our current understanding of the capabilities of the existing runners (see the [live version](/documentation/runners/capability-matrix/) for full details, descriptions, and Jira links); since integration is still under way, the system as whole isn’t yet in a completely stable, usable state. But that should be changing in the near future, and we’ll be updating loud and clear on this blog when the first supported Beam 1.0 release happens.

In the meantime, these tables should help clarify where we expect to be in the very near term, and help guide expectations about what existing runners are capable of, and what features runner implementers will be tackling next.

{{< capability-matrix-common >}}
{{< capability-matrix cap-data="capability-matrix-snapshot" cap-style="cap-summary" cap-view="blog" cap-other-view="full" cap-toggle-details=1 cap-display="block" >}}
