---
title:  "Google Summer of Code '19"
date:   2019-09-04 00:00:01 -0800
categories: 
  - blog 
  - gsoc
aliases:
  - /blog/2019/09/04/gsoc-19.html
authors:
  - ttanay

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


Google Summer of Code was an amazing learning experience for me.
I contributed to open source, learned about Apache Beam’s internals and worked with the best engineers in the world.

<!--more-->

## Motivation
Two of my friends had participated in GSoC in 2018. I was intrigued by their experience.
The idea of working on open-source software that could potentially be used by developers across the world, while being mentored by the best people in a field was exciting!
So, I decided to give Google Summer of Code a shot this year.

## What is Google Summer of Code?
[Google Summer of Code](https://summerofcode.withgoogle.com/) is a global program hosted by Google focused on introducing students to open source software development.
Students work on a 3 month programming project with an open source organization during their break from university.

## Why Apache Beam?
While interning at [Atlan](https://atlan.com/), I discovered the field of Data Engineering. I found the challenges and the discussions of the engineers there interesting. While researching for my internship project, I came across the Streaming Systems book. It introduced me to the unified model of Apache Beam for Batch and Streaming Systems, which I was fascinated by.
I wanted to explore Data Engineering, so for GSoC, I wanted to work on a project in that field. Towards the end of my internship, I started contributing to Apache Airflow(very cool project) and Apache Beam, hoping one of them would participate in GSoC. I got lucky!

[Also, Spotify’s Discover Weekly uses Apache Beam!](https://youtu.be/U2eWLb-LD44)

## Preparation
I had already read the [Streaming Systems book](http://streamingsystems.net/). So, I had an idea of the concepts that Beam is built on, but had never actually used Beam.
Before actually submitting a proposal, I went through a bunch of resources to make sure I had a concrete understanding of Beam.
I read the [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) and [Streaming 102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102) blogs by Tyler Akidau. They are the perfect introduction to Beam’s unified model for Batch and Streaming.
In addition, I watched all Beam talks on YouTube. You can find them on the [Beam Website](https://beam.apache.org/documentation/resources/videos-and-podcasts/).
Beam has really good documentation. The [Programming Guide](https://beam.apache.org/documentation/programming-guide/) lays out all of Beam’s concepts really well. [Beam’s execution model](https://beam.apache.org/documentation/runtime/model) is also documented well and is a must-read to understand how Beam processes data.
[waitingforcode.com](https://www.waitingforcode.com/apache-beam) also has good blog posts about Beam concepts.
To get a better sense of the Beam codebase, I played around with it and worked on some PRs to understand Beam better and got familiar with the test suite and workflows.

## GSoC Journey
GSoC has 2 phases. The first is the Community Bonding period in which students get familiar with the project and the community. The other is the actual Coding Period in which students work on their projects. Since the Coding Period has three evaluations spaced out by a month, I divided my project into three parts focusing on the implementation, tests, and documentation or improvements.

### Project
My project([BEAM-6611](https://issues.apache.org/jira/browse/BEAM-6611)) added support for File Loads method of inserting data into BigQuery for streaming pipelines. It builds on PR - [#7655](https://github.com/apache/beam/pull/7655) for [BEAM-6553](https://issues.apache.org/jira/browse/BEAM-6553) that added support in the Python SDK for writing to BigQuery using File Loads method for Batch pipelines. Streaming pipelines with non-default Windowing, Triggering and Accumulation mode can write data to BigQuery using file loads method. In case of failure, the pipeline will fail atomically. This means that each record will be loaded into BigQuery at-most-once.
You can find my proposal [here](https://docs.google.com/document/d/15Peyd3Z_wu5rvGWw8lMLpZuTyyreM_JOAEFFWvF97YY/edit?usp=sharing).

### Community Bonding
When GSoC started, my semester end exams had not yet finished. As a result, I couldn’t get much done. I worked on three PTransforms for the Python SDK - Latest, WithKeys and Reify.

### Coding Period I
In this period, I wrote some Integration Tests for the BigQuery sink using Streaming Inserts in streaming mode. I worked on a failing integration test for my project. I also finished the implementation of my project. But, one PostCommit test didn’t pass. I realized that the matcher for the Integration Test that queried BigQuery for the results was intended to be used in Batch mode. So, I wrote a version of the matcher to work in streaming mode.

### Coding Period II
Even after I had added the matcher for streaming mode, the PostComit tests did not pass. A test was being run even though it was not specified. I isolated the failure to a [limitation](https://nose.readthedocs.io/en/latest/doc_tests/test_multiprocess/multiprocess.html#other-differences-in-test-running) of the multiprocess plugin for [nose(a Python test framework)](https://nose.readthedocs.io/en/latest/) due to which it found more tests than had been specified. It took me a while to figure this out. In this period, changes for my project got merged.
I also worked on small issues related to testing.

This period was marked by a few exciting events:
 - Ending up in the top #100 contributors to apache/beam.
 - My first ever PR Review on an open source project.

<img src="https://pbs.twimg.com/media/D_XNSC-UIAUmswG?format=png&name=small" alt="Weird flex but ok" />

### Coding Period III
This was the final coding period before the program ended. Since my project was merged earlier than expected, my mentor suggested another issue([BEAM-7742](https://issues.apache.org/jira/browse/BEAM-7742)) in the same area - BigQueryIO, that I found interesting. So, I worked on partitioning written files in BigQuery to ensure that all load jobs triggered adhere to the load job size limitations specified for BigQuery.
While working on my project, I was using a pipeline that uses PubSub as a source and BigQuery as a sink to validate my changes. My mentor suggested we add them to the Beam test suite as it would be the ultimate test for BigQueryIO. I also worked on adding this test to Beam.

You can find the list of PRs I worked on [here](https://github.com/apache/beam/pulls?utf8=%E2%9C%93&q=is%3Apr+author%3Attanay).

## Conclusion
GSoC has been a lesson in discipline and goal-setting for me. Deciding what I wanted to work on and how much I wanted to get done each week was an important lesson.
I had never worked remotely, so this was a new experience. Although I struggled with it initially, I appreciate the flexibility that it comes with.
I also had a lot of fun learning about Apache Beam’s internals, and other tools in the same ecosystem.
This was also the first time I had written code with a test-first approach.

I thank my mentor - Pablo Estrada, Apache Beam, The Apache Software Foundation and Google Summer of Code for this opportunity. I am also grateful to my mentor for helping me with everything I needed and more, and the Apache Beam community for being supportive and encouraging.

With the right effort, perseverance, conviction, and a plan, anything is possible. Anything.
