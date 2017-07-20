---
layout: default
title: 'In Progress Work in Beam'
permalink: /contribute/work-in-progress/
---

# Work In Progress in the Apache Beam Project

As mentioned in the [Contribution Guide]({{ site.baseurl }}/contribute/contribution-guide/), all work in Beam is tracked in our [Apache JIRA](https://issues.apache.org/jira/browse/BEAM). In addition, the following types of work may be of particular interest to the Beam community.

* TOC
{:toc}


## Starter Tasks

The community regular tags good getting started tasks with the label `starter`. Use a quick [JIRA search](https://issues.apache.org/jira/issues?jql=project%20%3D%20BEAM%20AND%20status%20%3D%20Open%20AND%20labels%20%3D%20starter) to identify ways you can get started [contributing]({{ site.baseurl }}/contribute/contribution-guide/) to Beam.

## Feature Branches

Larger features with multiple active developers may be developed on a [feature branch]({{ site.baseurl }}/contribute/contribution-guide/#feature-branches) before being merged in the master branch. In particular, this is often used for initial development of new components like SDKs or runners.

Current branches include:

| Feature | Branch | JIRA Component | More Info |
| ---- | ---- | ---- | ---- |
| Apache Gearpump Runner | [gearpump-runner](https://github.com/apache/beam/tree/gearpump-runner) | [runner-gearpump](https://issues.apache.org/jira/browse/BEAM/component/12330829) | [runner homepage]({{ site.baseurl }}/documentation/runners/gearpump/) |
| Apache Spark 2.0 Runner | [runners-spark2](https://github.com/apache/beam/tree/runners-spark2) | - | [thread](https://lists.apache.org/thread.html/e38ac4e4914a6cb1b865b1f32a6ca06c2be28ea4aa0f6b18393de66f@%3Cdev.beam.apache.org%3E) |
| JStorm Runner | [jstorm-runner](https://github.com/apache/beam/tree/jstorm-runner) | [runner-jstorm](https://issues.apache.org/jira/browse/BEAM/component/12332477) | [BEAM-1899](https://issues.apache.org/jira/browse/BEAM-1899) |
| Beam SQL DSL | [DSL_SQL](https://github.com/apache/beam/tree/DSL_SQL) | [dsl-sql](https://issues.apache.org/jira/browse/BEAM/component/12332480) | [BEAM-301](https://issues.apache.org/jira/browse/BEAM-301) |
{:.table}

