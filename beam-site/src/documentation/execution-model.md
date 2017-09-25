---
layout: default
title: "Beam Execution Model"
permalink: /documentation/execution-model/
---

# Apache Beam Execution Model

The Beam model allows runners to execute your pipeline in different ways.
Depending on the runner’s choices, you may observe various effects as a result.
This page describes the effects of these choices so you can better understand
how Beam pipelines execute.

**Table of Contents:**
* TOC
{:toc}

## Processing of elements

The serialization and communication of elements between machines is one of the
most expensive operations in a distributed execution of your pipeline. Avoiding
this serialization may require re-processing elements after failures or may
limit the distribution of output to other machines.

The runner processes elements on many machines and may serialize elements in
between machines for other communication and persistence reasons.

### Serialization and communication

The runner may serialize elements for communication or persistence.

The runner may decide to transfer elements between transforms in a variety of
ways, such as:

1.  Routing elements to a worker for processing as part of a grouping operation.
    This may involve serializing elements and sorting them by their key.
1.  Redistributing elements between workers to adjust parallelism. This may
    involve serializing elements and communicating them to other workers.
1.  Using the elements in a side input to a `ParDo`. This may require
    serializing the elements and broadcasting them to all the workers executing
    the `ParDo`.
1.  Passing elements between transforms that are running on the same worker.
    This may avoid having to serialize the elements, and instead just passing
    them in memory.

Additionally, the runner may serialize and persist elements for other reasons:

1. When used as part of a Stateful `DoFn`, the runner may persist values to some
   state mechanism.
1. When committing the results of processing, the runner may persist the outputs
   as a checkpoint.

### Bundling and persistence

Beam pipelines often focus on ["embarassingly parallel"](https://en.wikipedia.org/wiki/Embarrassingly_parallel)
problems.  Because of this, the APIs emphasize processing elements in parallel,
which makes it difficult to express things like "assign a sequence number to
each element in a PCollection." This is intentional since such algorithms are
much more likely to suffer from scalability problems.

Processing all elements in parallel also has some drawbacks. Specifically, it
makes it impossible to batch any operations, such as writing elements to a sink
or checkpointing progress during processing.

Instead of processing all elements simultaneously, the elements in a
`PCollection` are processed in _bundles_. The division of the collection into
bundles is arbitrary and selected by the runner. This allows the runner to
choose an appropriate middle-ground between persisting results after every
element, and having to retry everything if there is a failure.

## Failures and parallelism within and between transforms

In this section, we discuss how elements in the input collection are processed
in parallel, and how transforms are retried when failures occur.

### Data-parallelism within one transform {#data-parallelism}

The bundling of elements when executing a single `ParDo` may look something like
this. In this diagram, a collection with 9 elements is divided into 2
bundles:

![bundling]({{ site.baseurl }}/images/execution_model_bundling.svg)

When the `ParDo` executes, these bundles may be processed in parallel worker
threads. The elements in each bundle are processed in sequence, as shown in this
diagram:

![bundling_gantt]({{ site.baseurl }}/images/execution_model_bundling_gantt.svg)

Since elements cannot be split, the maximum parallelism for a transform depends
on the number of elements in the collection. In this case, the maximum
parallelism is 9:

![bundling_gantt_max]({{ site.baseurl }}/images/execution_model_bundling_gantt_max.svg)

Note: Splittable ParDo allows splitting the processing of a single input across
multiple bundles. This feature is still a work in progress, but may already be
useful in some cases.

### Dependent-parallelism between transforms {#dependent-parallellism}

When two transforms are connected as shown below, the runner may choose to
execute them in a way such that the bundling of the two transforms are
dependent.

![bundling_multi]({{ site.baseurl }}/images/execution_model_bundling_multi.svg)

In this picture, `ParDo1` and `ParDo2` are _dependently parallel_ if the output
of `ParDo1` for a given element must be processed on the same worker thread.

![bundling_multi_gantt.svg]({{ site.baseurl }}/images/execution_model_bundling_multi_gantt.svg)

For example, two `ParDo` transforms in sequence may be _dependently parallel_ if
the runner chooses to execute the consuming transform on each output from the
producing transform without altering the bundling in between.

Executing transforms this way allows a runner to avoid redistributing elements
between workers, saving on communication costs. However, the maximum parallelism
now depends on the maximum parallelism of the first of the dependently parallel
steps.

### Failures within one transform

If processing of an element within a bundle fails, the entire bundle fails. The
elements in the bundle must be retried (otherwise the entire pipeline fails),
although they need not be retried with the same bundling.

In the following illustration, two elements (in green) were successfully
processed. The third element’s processing failed, and there are three elements
(in yellow) still to be processed. We see that the same elements were retried
and processing successfully completed. Note that as shown, the retry does not
necessarily happen in the same worker thread as the original attempt.

Because we encountered a failure while processing an element in the input
bundle, we had to reprocess all of the elements in the input bundle. Thus, the
entire output bundle must be thrown away since all of the results it contains
will be recomputed.

![failure_retry]({{ site.baseurl }}/images/execution_model_failure_retry.svg)

If the failed transform is a `ParDo`, then the `DoFn` instance is torn down and
abandoned.

### Coupled failure: Failures between transforms

If a failure to process an element in `ParDo2` causes `ParDo1` to re-execute,
these two steps are said to be _co-failing_.

In the following illustration, two `ParDo` transforms are processing elements.
While processing an element, the second `ParDo` fails (which is shown in red).
As a result, the runner must discard and recompute the output of the second
`ParDo`. Because the runner was executing the two `ParDo`s together, the output
bundle from the first `ParDo` must also be thrown away, and the elements in the
input bundle must be retried. These two `ParDo`s are co-failing.

![bundling_coupled failure]({{ site.baseurl }}/images/execution_model_bundling_coupled_failure.svg)

All `DoFns` that experience coupled failures are terminated and must be torn
down since they aren’t following the normal `DoFn` lifecycle .

Executing transforms this way allows a runner to avoid persisting elements
between transforms, saving on persistence costs.

