---
title:  "How We Added Windowing to the Apache Flink Batch Runner"
date:   2016-06-13 09:00:00 -0700
categories:
  - blog
aliases:
  - /blog/2016/06/13/flink-batch-runner-milestone.html
authors:
  - aljoscha
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
We recently achieved a major milestone by adding support for windowing to the [Apache Flink](https://flink.apache.org) Batch runner. In this post we would like to explain what this means for users of Apache Beam and highlight some of the implementation details.

<!--more-->

Before we start, though, let’s quickly talk about the execution of Beam programs and how this is relevant to today’s post. A Beam pipeline can contain bounded and unbounded sources. If the pipeline only contains bounded sources it can be executed in a batch fashion, if it contains some unbounded sources it must be executed in a streaming fashion. When executing a Beam pipeline on Flink, you don’t have to choose the execution mode. Internally, the Flink runner either translates the pipeline to a Flink `DataSet` program or a `DataStream` program, depending on whether unbounded sources are used in the pipeline. In the following, when we say “Batch runner” what we are really talking about is the Flink runner being in batch execution mode.

## What does this mean for users?

Support for windowing was the last missing puzzle piece for making the Flink Batch runner compatible with the Beam model. With the latest change to the Batch runner users can now run any pipeline that only contains bounded sources and be certain that the results match those of the original reference-implementation runners that were provided by Google as part of the initial code drop coming from the Google Dataflow SDK.

The most obvious part of the change is that windows can now be assigned to elements and that the runner respects these windows for the `GroupByKey` and `Combine` operations. A not-so-obvious change concerns side-inputs. In the Beam model, side inputs respect windows; when a value of the main input is being processed only the side input that corresponds to the correct window is available to the processing function, the `DoFn`.

Getting side-input semantics right is an important milestone in it’s own because it allows to use a big suite of unit tests for verifying the correctness of a runner implementation. These tests exercise every obscure detail of the Beam programming model and verify that the results produced by a runner match what you would expect from a correct implementation. In the suite, side inputs are used to compare the expected result to the actual result. With these tests being executed regularly we can now be more confident that the implementation produces correct results for user-specified pipelines.

## Under the Hood
The basis for the changes is the introduction of `WindowedValue` in the generated Flink transformations. Before, a Beam `PCollection<T>` would be transformed to a `DataSet<T>`. Now, we instead create a `DataSet<WindowedValue<T>>`. The `WindowedValue<T>` stores meta data about the value, such as the timestamp and the windows to which it was assigned.

With this basic change out of the way we just had to make sure that windows were respected for side inputs and that `Combine` and `GroupByKey` correctly handled windows. The tricky part there is the handling of merging windows such as session windows. For these we essentially emulate the behavior of a merging `WindowFn` in our own code.

After we got side inputs working we could enable the aforementioned suite of tests to check how well the runner behaves with respect to the Beam model. As can be expected there were quite some discrepancies but we managed to resolve them all. In the process, we also slimmed down the runner implementation. For example, we removed all custom translations for sources and sinks and are now relying only on Beam code for these, thereby greatly reducing the maintenance overhead.

## Summary
We reached a major milestone in adding windowing support to the Flink Batch runner, thereby making it compatible with the Beam model. Because of the large suite of tests that can now be executed on the runner we are also confident about the correctness of the implementation and about it staying that way in the future.
