---
title:  "Apache Beam: Six Months in Incubation"
date:   2016-08-03 00:00:01 -0700
categories:
  - blog
aliases:
  - /blog/2016/08/03/six-months.html
authors:
  - fjp
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

It’s been just over six months since Apache Beam was formally accepted into incubation with the [Apache Software Foundation](http://www.apache.org). As a community, we’ve been hard at work getting Beam off the ground.

<!--more-->

Looking just at raw numbers for those first six months, that’s:

* 48,238 lines of preexisting code donated by Cloudera, dataArtisans, and Google.
* 761 pull requests from 45 contributors.
* 498 Jira issues opened and 245 resolved.
* 1 incubating release (and another 1 in progress). 
* 4,200 hours of automated tests. 
* 161 subscribers / 606 messages on user@.
* 217 subscribers / 1205 messages on dev@.
* 277 stars and 174 forks on GitHub.

And behind those numbers, there’s been a ton of technical progress, including:

* Refactoring of the entire codebase, examples, and tests to be truly runner-independent.
* New functionality in the Apache Flink runner for timestamps/windows in batch and bounded sources and side inputs in streaming mode.
* Work in progress to upgrade the Apache Spark runner to use Spark 2.0.
* Several new runners from the wider Apache community -- Apache Gearpump has its own feature branch, Apache Apex has a PR, and conversations are starting on Apache Storm and others.
* New SDKs/DSLs for exposing the Beam model -- the Python SDK from Google is in on a feature branch, and there are plans to add the Scio DSL from Spotify.
* Support for additional data sources and sinks -- Apache Kafka and JMS are in, there are PRs for Amazon Kinesis, Apache Cassandra, and MongoDB, and more connectors are being planned.

But perhaps most importantly, we’re committed to building an involved, welcoming community. So far, we’ve:

* Started building a vibrant developer community, with detailed design discussions on features like DoFn reuse semantics, serialization technology, and an API for accessing state.
* Started building a user community with an active mailing list and improvements to the website and documentation.
* Had multiple talks on Beam at venues including ApacheCon, Hadoop Summit, Kafka Summit, JBCN Barcelona, and Strata.
* Presented at multiple existing meetups and are starting to organize some of our own.

While it’s nice to reflect back on all we’ve done, we’re working full _stream_ ahead towards a stable release and graduation from incubator. And we’d love your help -- join the [mailing lists](/get-started/support/), check out the [contribution guide](/contribute/contribution-guide/), and grab a [starter task](https://issues.apache.org/jira/browse/BEAM-520?jql=project%20%3D%20BEAM%20AND%20resolution%20%3D%20Unresolved%20AND%20labels%20in%20(newbie%2C%20starter)) from Jira!

