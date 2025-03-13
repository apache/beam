---
title: "Pattern for dynamically grouping elements"
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

# Dynamically grouping elements using the `BatchElements`-transform

## Why Batch Elements?
Apache Beam provides the `BatchElements` [transform](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highlight=batchelements#apache_beam.transforms.util.BatchElements) to amortize processing time of following operations. This is best applied ahead of operations that have a substantial fixed cost per invocation and a smaller cost per element processed within that invocation. Put simply, batching elements is an optimization step ahead of operations that are more efficient when handling multiple elements at once when compared to processing them one at a time.

### Example: Batching in RunInference
In the context of `RunInference`, batching elements largely serves to reduce the number of calls being made to the model used for inference. This is important for efficiency in on-worker inference contexts, but has an even greater impact when looking at inference calls being sent to a remote service. When API rate limiting is a consideration, batching elements as possible is an important way to reduce the risk of overrunning quotas. The `RunInference` [framework](https://github.com/apache/beam/blob/f3df03d8fa9eea9769260783a8daffb2de2d33f6/sdks/python/apache_beam/ml/inference/base.py#L987) always [calls into](https://github.com/apache/beam/blob/f3df03d8fa9eea9769260783a8daffb2de2d33f6/sdks/python/apache_beam/ml/inference/base.py#L1118) `BatchElements`, further highlighting how essential this step is for inference workloads.

## Batching Within Bundles
The main methodology used for batching elements is to batch within bundles of elements. Bundles are iterated through, appending elements to a bundle before reaching a target batch size and then being emitted. Elements in the same batch are guaranteed to exist in the same event time window, as the batching function is window-aware. If there is an incomplete batch buffered when the end of the bundle is reached, it is emitted.

### Parameter Tuning Considerations
The majority of tuning for batching in this manner revolves around parameters used to estimate the optimal batch size based on downstream performance. The `_BatchSizeEstimator` [object](https://github.com/apache/beam/blob/3063b55757509dad1c14751c9f2aa5905826d9a0/sdks/python/apache_beam/transforms/util.py#L309) maintains data points around batch size and clock time for emitted bundles, then attempts to solve for the current ideal batch size using linear regression. The [docstring](https://github.com/apache/beam/blob/3063b55757509dad1c14751c9f2aa5905826d9a0/sdks/python/apache_beam/transforms/util.py#L785) for `BatchElements` explains this and the parameters handled here in detail. The most important considerations for most users are the minimum and maximum batch sizes (setting them to be the same value produces fixed batch sizes rather than dynamic batch sizes) and the element size function. The latter allows users to define a lambda that can size each input individually rather than simply counting each element as a size of 1. This can be extremely useful if inputs have noticeably different processing times when passed to the model; for example, sending bodies of text to a model could be sized by character lengths.

## Batching Across Bundles
Batching elements within bundles has one major downside: if bundles are small (or even single elements) the operation is effectively a no-op. This is especially true in streaming pipelines, where we expect bundles to only contain 1 to 2 elements. To enable batching with small bundles, we instead have to be able to batch elements across bundles. For an in-depth technical explanation of how this works, the design document for [batching across bundles](https://docs.google.com/document/d/1Rin_5Vm3qT1Mkb5PcHgTDrjXc3j0Admzi3fEGEHB2-4/edit?usp=sharing) (also known as stateful batching) outlines the code; however, the high-level explanation is that we leverage Beam’s [State API](documentation/programming-guide/#types-of-state) to store elements across bundles, emitting them downstream when we either reach the desired bundle size or have kept the bundle buffered for some maximum amount of time.

To enable stateful batching, pass the `max_batch_duration_secs` [parameter](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highlight=batchelements#apache_beam.transforms.util.BatchElements) to `BatchElements` in Apache Beam version 2.52.0 or later. This will key the input elements to ensure the state API can be used and then use the stateful batching function. Batches will be emitted when they either reach the current target batch size or the batch has been buffered for an amount of time greater than or equal to `max_batch_duration_secs`. It should be noted that the time-based behavior is implemented using the [Timers API](/documentation/programming-guide/#timers) and is best-effort, so the actual duration a batch may be held before being emitted can be greater than the set maximum.

Batching across bundles does have its own downsides:
* Buffering batches does introduce a bottleneck into the pipeline, reducing throughput compared batching within bundles
* The keying step that allows the State API to function can shuffle data around on the workers, which can incur a large amount of network traffic between workers if the data being processed is large

### Parameter Tuning Considerations
In addition to all of the tuning parameters used for batching within bundles, tuning the `max_batch_duration_secs` parameter will have a significant impact on the throughput of the transform. The trade-off being made when selecting a max batch duration is between throughput and consistent batch sizes. Larger values will generally have reduced throughput, as incomplete batches will be held for longer before being sent to the inference call itself; however, this will produce full batches more consistently. Smaller values, on the other hand, will generally have higher throughput, but may have smaller batch sizes as a consequence. Which of these you value over the other depends on your use-case.

It should also be noted that these trends are not always going to be true! In streaming contexts, there’s a reasonable amount of variance in how often elements are ingested into the pipeline. You may be able to reach a desired batch size with a low maximum batch duration, but that is different from consistently reaching that batch size with a longer maximum duration. It’s best to reason about these described tradeoffs as the average case.