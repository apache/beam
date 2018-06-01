---
layout: post
title:  "Dynamic work rebalancing for Beam"
date:   2016-05-18 11:00:00 -0700
excerpt_separator: <!--more-->
categories: blog
authors:
  - dhalperi
---

This morning, Eugene and Malo from the Google Cloud Dataflow team posted [*No shard left behind: dynamic work rebalancing in Google Cloud Dataflow*](https://cloud.google.com/blog/big-data/2016/05/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow). This article discusses Cloud Dataflow’s solution to the well-known straggler problem.

<!--more-->

In a large batch processing job with many tasks executing in parallel, some of the tasks -- the stragglers -- can take a much longer time to complete than others, perhaps due to imperfect splitting of the work into parallel chunks when issuing the job. Typically, waiting for stragglers means that the overall job completes later than it should, and may also reserve too many machines that may be underutilized at the end. Cloud Dataflow’s dynamic work rebalancing can mitigate stragglers in most cases.

What I’d like to highlight for the Apache Beam (incubating) community is that Cloud Dataflow’s dynamic work rebalancing is implemented using *runner-specific* control logic on top of Beam’s *runner-independent* [`BoundedSource API`](https://github.com/apache/beam/blob/9fa97fb2491bc784df53fb0f044409dbbc2af3d7/sdks/java/core/src/main/java/org/apache/beam/sdk/io/BoundedSource.java). Specifically, to steal work from a straggler, a runner need only call the reader’s [`splitAtFraction method`](https://github.com/apache/beam/blob/3edae9b8b4d7afefb5c803c19bb0a1c21ebba89d/sdks/java/core/src/main/java/org/apache/beam/sdk/io/BoundedSource.java#L266). This will generate a new source containing leftover work, and then the runner can pass that source off to another idle worker. As Beam matures, I hope that other runners are interested in figuring out whether these APIs can help them improve performance, implementing dynamic work rebalancing, and collaborating on API changes that will help solve other pain points.
