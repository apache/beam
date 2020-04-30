---
title: "Execution model"
aliases:
  - /documentation/execution-model/
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

# Execution model

The Beam model allows runners to execute your pipeline in different ways. You
may observe various effects as a result of the runner’s choices. This page
describes these effects so you can better understand how Beam pipelines execute.

## Processing of elements

The serialization and communication of elements between machines is one of the
most expensive operations in a distributed execution of your pipeline. Avoiding
this serialization may require re-processing elements after failures or may
limit the distribution of output to other machines.

### Serialization and communication

The runner might serialize elements between machines for communication purposes
and for other reasons such as persistence.

A runner may decide to transfer elements between transforms in a variety of
ways, such as:

*   Routing elements to a worker for processing as part of a grouping operation.
    This may involve serializing elements and grouping or sorting them by their
    key.
*   Redistributing elements between workers to adjust parallelism. This may
    involve serializing elements and communicating them to other workers.
*   Using the elements in a side input to a `ParDo`. This may require
    serializing the elements and broadcasting them to all the workers executing
    the `ParDo`.
*   Passing elements between transforms that are running on the same worker.
    This may allow the runner to avoid serializing elements; instead, the runner
    can just pass the elements in memory.

Some situations where the runner may serialize and persist elements are:

1. When used as part of a stateful `DoFn`, the runner may persist values to some
   state mechanism.
1. When committing the results of processing, the runner may persist the outputs
   as a checkpoint.

### Bundling and persistence

Beam pipelines often focus on "[embarassingly parallel](https://en.wikipedia.org/wiki/embarrassingly_parallel)"
problems. Because of this, the APIs emphasize processing elements in parallel,
which makes it difficult to express actions like "assign a sequence number to
each element in a PCollection". This is intentional as such algorithms are much
more likely to suffer from scalability problems.

Processing all elements in parallel also has some drawbacks. Specifically, it
makes it impossible to batch any operations, such as writing elements to a sink
or checkpointing progress during processing.

Instead of processing all elements simultaneously, the elements in a
`PCollection` are processed in _bundles_. The division of the collection into
bundles is arbitrary and selected by the runner. This allows the runner to
choose an appropriate middle-ground between persisting results after every
element, and having to retry everything if there is a failure. For example, a
streaming runner may prefer to process and commit small bundles, and a batch
runner may prefer to process larger bundles.

## Failures and parallelism within and between transforms {#parallelism}

In this section, we discuss how elements in the input collection are processed
in parallel, and how transforms are retried when failures occur.

### Data-parallelism within one transform {#data-parallelism}

When executing a single `ParDo`, a runner might divide an example input
collection of nine elements into two bundles as shown in figure 1.

![Bundle A contains five elements. Bundle B contains four elements.](/images/execution_model_bundling.svg)

*Figure 1: A runner divides an input collection into two bundles.*

When the `ParDo` executes, workers may process the two bundles in parallel as
shown in figure 2.

![Two workers process the two bundles in parallel. Worker one processes bundle A. Worker two processes bundle B.](/images/execution_model_bundling_gantt.svg)

*Figure 2: Two workers process the two bundles in parallel.*

Since elements cannot be split, the maximum parallelism for a transform depends
on the number of elements in the collection. In figure 3, the input collection
has nine elements, so the maximum parallelism is nine.

![Nine workers process a nine element input collection in parallel.](/images/execution_model_bundling_gantt_max.svg)

*Figure 3: Nine workers process a nine element input collection in parallel.*

Note: Splittable ParDo allows splitting the processing of a single input across
multiple bundles. This feature is a work in progress.

### Dependent-parallelism between transforms {#dependent-parallellism}

`ParDo` transforms that are in sequence may be _dependently parallel_ if the
runner chooses to execute the consuming transform on the producing transform's
output elements without altering the bundling. In figure 4, `ParDo1` and
`ParDo2` are _dependently parallel_ if the output of `ParDo1` for a given
element must be processed on the same worker.

![ParDo1 processes an input collection that contains bundles A and B. ParDo2 then processes the output collection from ParDo1, which contains bundles C and D.](/images/execution_model_bundling_multi.svg)

*Figure 4: Two transforms in sequence and their corresponding input collections.*

Figure 5 shows how these dependently parallel transforms might execute. The
first worker executes `ParDo1` on the elements in bundle A (which results in
bundle C), and then executes `ParDo2` on the elements in bundle C. Similarly,
the second worker executes `ParDo1` on the elements in bundle B (which results
in bundle D), and then executes `ParDo2` on the elements in bundle D.

![Worker one executes ParDo1 on bundle A and Pardo2 on bundle C. Worker two executes ParDo1 on bundle B and ParDo2 on bundle D.](/images/execution_model_bundling_multi_gantt.svg)

*Figure 5: Two workers execute dependently parallel ParDo transforms.*

Executing transforms this way allows a runner to avoid redistributing elements
between workers, which saves on communication costs. However, the maximum parallelism
now depends on the maximum parallelism of the first of the dependently parallel
steps.

### Failures within one transform

If processing of an element within a bundle fails, the entire bundle fails. The
elements in the bundle must be retried (otherwise the entire pipeline fails),
although they do not need to be retried with the same bundling.

For this example, we will use the `ParDo` from figure 1 that has an input
collection with nine elements and is divided into two bundles.

In figure 6, the first worker successfully processes all five elements in bundle
A. The second worker processes the four elements in bundle B: the first two
elements were successfully processed, the third element’s processing failed, and
there is one element still awaiting processing.

We see that the runner retries all elements in bundle B and the processing
completes successfully the second time. Note that the retry does not necessarily
happen on the same worker as the original processing attempt, as shown in the
figure.

![Worker two fails to process an element in bundle B. Worker one finishes processing bundle A and then successfully retries to execute bundle B.](/images/execution_model_failure_retry.svg)

*Figure 6: The processing of an element within bundle B fails, and another worker
retries the entire bundle.*

Because we encountered a failure while processing an element in the input
bundle, we had to reprocess _all_ of the elements in the input bundle. This means
the runner must throw away the entire output bundle since all of the results it
contains will be recomputed.

Note that if the failed transform is a `ParDo`, then the `DoFn` instance is torn
down and abandoned.

### Coupled failure: Failures between transforms {#coupled-failure}

If a failure to process an element in `ParDo2` causes `ParDo1` to re-execute,
these two steps are said to be _co-failing_.

For this example, we will use the two `ParDo`s from figure 4.

In figure 7, worker two successfully executes `ParDo1` on all elements in bundle
B. However, the worker fails to process an element in bundle D, so `ParDo2`
fails (shown as the red X). As a result, the runner must discard and recompute
the output of `ParDo2`. Because the runner was executing `ParDo1` and `ParDo2`
together, the output bundle from `ParDo1` must also be thrown away, and all
elements in the input bundle must be retried. These two `ParDo`s are co-failing.

![Worker two fails to process en element in bundle D, so all elements in both bundle B and bundle D must be retried.](/images/execution_model_bundling_coupled_failure.svg)

*Figure 7: Processing of an element within bundle D fails, so all elements in
the input bundle are retried.*

Note that the retry does not necessarily have the same processing time as the
original attempt, as shown in the diagram.

All `DoFns` that experience coupled failures are terminated and must be torn
down since they aren’t following the normal `DoFn` lifecycle .

Executing transforms this way allows a runner to avoid persisting elements
between transforms, saving on persistence costs.