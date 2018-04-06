---
layout: section
title: 'In Progress Work in Beam'
section_menu: section-menu/contribute.html
permalink: /contribute/work-in-progress/
---

# Work In Progress in the Apache Beam Project

As mentioned in the [Contribution Guide]({{ site.baseurl }}/contribute/contribution-guide/), all work in Beam is tracked in our [Apache JIRA](https://issues.apache.org/jira/browse/BEAM). In addition, the following types of work may be of particular interest to the Beam community.

* TOC
{:toc}


## Starter Tasks

The community regular tags good getting started tasks with the label `starter`. Use a quick [JIRA search](https://issues.apache.org/jira/issues?jql=project%20%3D%20BEAM%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20starter) to identify ways you can get started [contributing]({{ site.baseurl }}/contribute/contribution-guide/) to Beam.

## Feature Branches

Larger features with multiple active developers may be developed on a [feature branch]({{ site.baseurl }}/contribute/feature-branches/) before being merged in the master branch. In particular, this is often used for initial development of new components like SDKs or runners.

Current branches include:

| Feature | Branch | JIRA Component | More Info |
| ---- | ---- | ---- | ---- |
| Apache Spark 2.0 Runner | [runners-spark2](https://github.com/apache/beam/tree/runners-spark2) | - | [thread](https://lists.apache.org/thread.html/e38ac4e4914a6cb1b865b1f32a6ca06c2be28ea4aa0f6b18393de66f@%3Cdev.beam.apache.org%3E) |
| [JStorm Runner]({{ site.baseurl }}/documentation/runners/jstorm) | [jstorm-runner](https://github.com/apache/beam/tree/jstorm-runner) | [runner-jstorm](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-jstorm) | [BEAM-1899](https://issues.apache.org/jira/browse/BEAM-1899) |
| MapReduce Runner | [mr-runner](https://github.com/apache/beam/tree/mr-runner) | [runner-mapreduce](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-mapreduce) | [BEAM-165](https://issues.apache.org/jira/browse/BEAM-165) |
| Tez Runner | [tez-runner](https://github.com/apache/beam/tree/tez-runner) | [runner-tez](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-tez) | [BEAM-2709](https://issues.apache.org/jira/browse/BEAM-2709) |
| Go SDK | master | [sdk-go](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go) | [BEAM-2083](https://issues.apache.org/jira/browse/BEAM-2083) |
{:.table}
