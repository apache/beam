---
title:  "Apache Beam is applying to Season of Docs"
date:   2019-04-19 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/04/19/season-of-docs.html
authors:
        - aizhamal

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


The Apache Beam community is thrilled to announce its application to the first edition of  Season of Docs 2019! 

<!--more-->

<img src="/images/blog/SoD.png" alt="Season of Docs 2019 flyer" height="455" width="640" >

[Season of Docs](https://developers.google.com/season-of-docs/) is a unique program that pairs technical writers with open source mentors to contribute to open source. This creates an opportunity to introduce the technical writer to an open source community and provide guidance while the writer works on a real world open source project. We, in the Apache Beam community, would love to take this chance and invite technical writers to collaborate with us, and help us improve our documentation in many ways.

Apache Beam does have help from excellent technical writers, but the documentation needs of the project often exceed their bandwidth. This is why we are excited about this program.

After discussing ideas in the community, we have been able to find mentors, and frame two ideas that we think would be a great fit for an incoming tech writer to tackle. We hope you will find this opportunity interesting - and if you do, please get in touch by emailing the Apache Beam mailing list at [dev@beam.apache.org](mailto:dev@beam.apache.org) (you will need to subscribe first by emailing to [dev-subscribe@beam.apache.org](mailto:dev-subscribe@beam.apache.org)).  

The project ideas available in Apache Beam are described below. Please take a look and ask any questions that you may have. We will be very happy to help you get onboarded with the project.

### Project ideas

**Deployment of Flink and Spark Clusters for use with Portable Beam**

The Apache Beam vision has been to provide a framework for users to write and execute pipelines on the programming language of your choice, and the runner of your choice. As the reality of Beam has evolved towards this vision, the way in which Beam is run on top of runners such as Apache Spark and Apache Flink has changed.

These changes are documented in the wiki and in design documents, and are accessible for Beam contributors; but they are not available in the user-facing documentation. This has been a barrier of adoption for other users of Beam.

This project involves improving the [Flink Runner page](https://beam.apache.org/documentation/runners/flink/ ) to include strategies to deploy Beam on a few different environments: A Kubernetes cluster, a Google Cloud Dataproc cluster, and an AWS EMR cluster. There are other places in the documentation that should be updated in this regard, such as the [Python streaming](https://beam.apache.org/documentation/sdks/python-streaming/) section, and the [set of supported features](https://beam.apache.org/documentation/sdks/python-streaming/#unsupported-features).

After working on the Flink Runner, then similar updates should be made to the [Spark Runner page](https://beam.apache.org/documentation/runners/spark/), and the [getting started documentation](https://beam.apache.org/get-started/beam-overview/).


**The runner comparison page / capability matrix update**

Beam maintains a [capability matrix](https://beam.apache.org/documentation/runners/capability-matrix/) to track which Beam features are supported by which set of language SDKs + Runners.
This project involves a number of [corrections and improvements to the capability matrix](https://issues.apache.org/jira/browse/BEAM-2888 ); followed by a few larger set of changes, involving:

- Plain english summaries for each runner’s support of the Beam model.
- A paragraph-length description of the production-readiness for each runner.
- Comparisons for non-model differences between runners.
- Comparison for support of the portability framework for each runner.


Thank you, and we are looking forward to hearing from you!
