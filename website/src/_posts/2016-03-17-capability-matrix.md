---
layout: post
title:  "Clarifying & Formalizing Runner Capabilities"
date:   2016-03-17 11:00:00 -0700
excerpt_separator: <!--more-->
categories: beam capability
authors:
  - fjp
  - takidau

capability-matrix-snapshot:
  columns:
    - class: model
      name: Beam Model
    - class: dataflow
      name: Google Cloud Dataflow
    - class: flink
      name: Apache Flink
    - class: spark
      name: Apache Spark
  categories:
    - description: What is being computed?
      anchor: what
      color-b: 'ca1'
      color-y: 'ec3'
      color-p: 'fe5'
      color-n: 'ddd'
      rows:
        - name: ParDo
          values:
            - class: model
              l1: 'Yes'
              l2: element-wise processing
              l3: Element-wise transformation parameterized by a chunk of user code. Elements are processed in bundles, with initialization and termination hooks. Bundle size is chosen by the runner and cannot be controlled by user code. ParDo processes a main input PCollection one element at a time, but provides side input access to additional PCollections.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: Batch mode uses large bundle sizes. Streaming uses smaller bundle sizes.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: ParDo itself, as per-element transformation with UDFs, is fully supported by Flink for both batch and streaming.
            - class: spark
              l1: 'Yes'
              l2: fully supported
              l3: ParDo applies per-element transformations as Spark FlatMapFunction.
        - name: GroupByKey
          values:
            - class: model
              l1: 'Yes'
              l2: key grouping
              l3: Grouping of key-value pairs per key, window, and pane. (See also other tabs.)
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "Uses Flink's keyBy for key grouping. When grouping by window in streaming (creating the panes) the Flink runner uses the Beam code. This guarantees support for all windowing and triggering mechanisms."
            - class: spark
              l1: 'Partially'
              l2: group by window in batch only
              l3: "Uses Spark's groupByKey for grouping. Grouping by window is currently only supported in batch."
        - name: Flatten
          values:
            - class: model
              l1: 'Yes'
              l2: collection concatenation
              l3: Concatenates multiple homogenously typed collections together.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: ''
            - class: spark
              l1: 'Yes'
              l2: fully supported
              l3: ''
              
        - name: Combine
          values:
            - class: model
              l1: 'Yes'
              l2: associative &amp; commutative aggregation
              l3: 'Application of an associative, commutative operation over all values ("globally") or over all values associated with each key ("per key"). Can be implemented using ParDo, but often more efficient implementations exist.'
            - class: dataflow
              l1: 'Yes'
              l2: 'efficient execution'
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: 'fully supported'
              l3: Uses a combiner for pre-aggregation for batch and streaming.
            - class: spark
              l1: 'Yes'
              l2: fully supported
              l3: Supports GroupedValues, Globally and PerKey.

        - name: Composite Transforms
          values:
            - class: model
              l1: 'Yes'
              l2: user-defined transformation subgraphs
              l3: Allows easy extensibility for library writers.  In the near future, we expect there to be more information provided at this level -- customized metadata hooks for monitoring, additional runtime/environment hooks, etc.
            - class: dataflow
              l1: 'Partially'
              l2: supported via inlining
              l3: Currently composite transformations are inlined during execution. The structure is later recreated from the names, but other transform level information (if added to the model) will be lost.
            - class: flink
              l1: 'Partially'
              l2: supported via inlining
              l3: ''
            - class: spark
              l1: 'Partially'
              l2: supported via inlining
              l3: ''

        - name: Side Inputs
          values:
            - class: model
              l1: 'Yes'
              l2: additional elements available during DoFn execution
              l3: Side inputs are additional <tt>PCollections</tt> whose contents are computed during pipeline execution and then made accessible to DoFn code. The exact shape of the side input depends both on the <tt>PCollectionView</tt> used to describe the access pattern (interable, map, singleton) and the window of the element from the main input that is currently being processed.
            - class: dataflow
              l1: 'Yes'
              l2: some size restrictions in streaming
              l3: Batch implemented supports a distributed implementation, but streaming mode may force some size restrictions. Neither mode is able to push lookups directly up into key-based sources.
            - class: flink
              jira: BEAM-102
              l1: 'Partially'
              l2: no supported in streaming
              l3: Supported in batch. Side inputs for streaming are currently WiP.
            - class: spark
              l1: 'Partially'
              l2: not supported in streaming
              l3: "Side input is actually a broadcast variable in Spark so it can't be updated during the life of a job. Spark-runner implementation of side input is more of an immutable, static, side input."

        - name: Source API
          values:
            - class: model
              l1: 'Yes'
              l2: user-defined sources
              l3: Allows users to provide additional input sources. Supports both bounded and unbounded data. Includes hooks necessary to provide efficient parallelization (size estimation, progress information, dynamic splitting, etc).
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: 
            - class: flink
              jira: BEAM-103
              l1: 'Partially'
              l2: parallelism 1 in streaming
              l3: Fully supported in batch. In streaming, sources currently run with parallelism 1.
            - class: spark
              l1: 'Yes'
              l2: fully supported
              l3: 
              
        - name: Aggregators
          values:
            - class: model
              l1: 'Partially'
              l2: user-provided metrics
              l3: Allow transforms to aggregate simple metrics across bundles in a <tt>DoFn</tt>. Semantically equivalent to using a side output, but support partial results as the transform executes. Will likely want to augment <tt>Aggregators</tt> to be more useful for processing unbounded data by making them windowed.
            - class: dataflow
              l1: 'Partially'
              l2: may miscount in streaming mode
              l3: Current model is fully supported in batch mode. In streaming mode, <tt>Aggregators</tt> may under or overcount when bundles are retried.
            - class: flink
              l1: 'Partially'
              l2: may undercount in streaming
              l3: Current model is fully supported in batch. In streaming mode, <tt>Aggregators</tt> may undercount.
            - class: spark
              l1: 'Partially'
              l2: streaming requires more testing
              l3: "Uses Spark's <tt>AccumulatorParam</tt> mechanism"

        - name: Keyed State
          values:
            - class: model
              jira: BEAM-25
              l1: 'No'
              l2: storage per key, per window
              l3: Allows fine-grained access to per-key, per-window persistent state. Necessary for certain use cases (e.g. high-volume windows which store large amounts of data, but typically only access small portions of it; complex state machines; etc.) that are not easily or efficiently addressed via <tt>Combine</tt> or <tt>GroupByKey</tt>+<tt>ParDo</tt>. 
            - class: dataflow
              l1: 'No'
              l2: pending model support
              l3: Dataflow already supports keyed state internally, so adding support for this should be easy once the Beam model exposes it.
            - class: flink
              l1: 'No'
              l2: pending model support
              l3: Flink already supports keyed state, so adding support for this should be easy once the Beam model exposes it.
            - class: spark
              l1: 'No'
              l2: pending model support
              l3: Spark supports keyed state with mapWithState() so support shuold be straight forward.
              
              
    - description: Where in event time?
      anchor: where
      color-b: '37d'
      color-y: '59f'
      color-p: '8cf'
      color-n: 'ddd'
      rows:
        - name: Global windows
          values:
            - class: model
              l1: 'Yes'
              l2: all time
              l3: The default window which covers all of time. (Basically how traditional batch cases fit in the model.)
            - class: dataflow
              l1: 'Yes'
              l2: default
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: spark
              l1: 'Yes'
              l2: supported
              l3: ''
              
        - name: Fixed windows
          values:
            - class: model
              l1: 'Yes'
              l2: periodic, non-overlapping
              l3: Fixed-size, timestamp-based windows. (Hourly, Daily, etc)
            - class: dataflow
              l1: 'Yes'
              l2: built-in
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: spark
              l1: Partially
              l2: currently only supported in batch
              l3: ''
              
        - name: Sliding windows
          values:
            - class: model
              l1: 'Yes'
              l2: periodic, overlapping
              l3: Possibly overlapping fixed-size timestamp-based windows (Every minute, use the last ten minutes of data.)
            - class: dataflow
              l1: 'Yes'
              l2: built-in
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''

        - name: Session windows
          values:
            - class: model
              l1: 'Yes'
              l2: activity-based
              l3: Based on bursts of activity separated by a gap size. Different per key.
            - class: dataflow
              l1: 'Yes'
              l2: built-in
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: pending Spark engine support
              l3: ''

        - name: Custom windows
          values:
            - class: model
              l1: 'Yes'
              l2: user-defined windows
              l3: All windows must implement <tt>BoundedWindow</tt>, which specifies a max timestamp. Each <tt>WindowFn</tt> assigns elements to an associated window.
            - class: dataflow
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: pending Spark engine support
              l3: ''

        - name: Custom merging windows
          values:
            - class: model
              l1: 'Yes'
              l2: user-defined merging windows
              l3: A custom <tt>WindowFn</tt> additionally specifies whether and how to merge windows.
            - class: dataflow
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: pending Spark engine support
              l3: ''

        - name: Timestamp control
          values:
            - class: model
              l1: 'Yes'
              l2: output timestamp for window panes
              l3: For a grouping transform, such as GBK or Combine, an OutputTimeFn specifies (1) how to combine input timestamps within a window and (2) how to merge aggregated timestamps when windows merge.
            - class: dataflow
              l1: 'Yes'
              l2: supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: pending Spark engine support
              l3: ''


              
    - description: When in processing time?
      anchor: when
      color-b: '6a4'
      color-y: '8c6'
      color-p: 'ae8'
      color-n: 'ddd'
      rows:
        
        - name: Configurable triggering
          values:
            - class: model
              l1: 'Yes'
              l2: user customizable
              l3: Triggering may be specified by the user (instead of simply driven by hardcoded defaults).
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: Fully supported in streaming mode. In batch mode, intermediate trigger firings are effectively meaningless.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''

        - name: Event-time triggers
          values:
            - class: model
              l1: 'Yes'
              l2: relative to event time
              l3: Triggers that fire in response to event-time completeness signals, such as watermarks progressing.
            - class: dataflow
              l1: 'Yes'
              l2: yes in streaming, fixed granularity in batch
              l3: Fully supported in streaming mode. In batch mode, currently watermark progress jumps from the beginning of time to the end of time once the input has been fully consumed, thus no additional triggering granularity is available.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''
              
        - name: Processing-time triggers
          values:
            - class: model
              l1: 'Yes'
              l2: relative to processing time
              l3: Triggers that fire in response to processing-time advancing.
            - class: dataflow
              l1: 'Yes'
              l2: yes in streaming, fixed granularity in batch
              l3: Fully supported in streaming mode. In batch mode, from the perspective of triggers, processing time currently jumps from the beginning of time to the end of time once the input has been fully consumed, thus no additional triggering granularity is available.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'Yes'
              l2: "This is Spark streaming's native model"
              l3: "Spark processes streams in micro-batches. The micro-batch size is actually a pre-set, fixed, time interval. Currently, the runner takes the first window size in the pipeline and sets it's size as the batch interval. Any following window operations will be considered processing time windows and will affect triggering."
              
        - name: Count triggers
          values:
            - class: model
              l1: 'Yes'
              l2: every N elements
              l3: Triggers that fire after seeing at least N elements.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: Fully supported in streaming mode. In batch mode, elements are processed in the largest bundles possible, so count-based triggers are effectively meaningless.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''

        - name: '[Meta]data driven triggers'
          values:
            - class: model
              jira: BEAM-101
              l1: 'No'
              l2: in response to data
              l3: Triggers that fire in response to attributes of the data being processed.
            - class: dataflow
              l1: 'No'
              l2: pending model support
              l3: 
            - class: flink
              l1: 'No'
              l2: pending model support
              l3: 
            - class: spark
              l1: 'No'
              l2: pending model support
              l3: 

        - name: Composite triggers
          values:
            - class: model
              l1: 'Yes'
              l2: compositions of one or more sub-triggers
              l3: Triggers which compose other triggers in more complex structures, such as logical AND, logical OR, early/on-time/late, etc.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''
              
        - name: Allowed lateness
          values:
            - class: model
              l1: 'Yes'
              l2: event-time bound on window lifetimes
              l3: A way to bound the useful lifetime of a window (in event time), after which any unemitted results may be materialized, the window contents may be garbage collected, and any addtional late data that arrive for the window may be discarded.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: Fully supported in streaming mode. In batch mode no data is ever late.
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''
              
        - name: Timers
          values:
            - class: model
              jira: BEAM-27
              l1: 'No'
              l2: delayed processing callbacks
              l3: A fine-grained mechanism for performing work at some point in the future, in either the event-time or processing-time domain. Useful for orchestrating delayed events, timeouts, etc in complex state per-key, per-window state machines.
            - class: dataflow
              l1: 'No'
              l2: pending model support
              l3: Dataflow already supports timers internally, so adding support for this should be easy once the Beam model exposes it.
            - class: flink
              l1: 'No'
              l2: pending model support
              l3: Flink already supports timers internally, so adding support for this should be easy once the Beam model exposes it.
            - class: spark
              l1: 'No'
              l2: pending model support
              l3: ''
              
              
    - description: How do refinements relate?
      anchor: how
      color-b: 'b55'
      color-y: 'd77'
      color-p: 'faa'
      color-n: 'ddd'
      rows:
        
        - name: Discarding
          values:
            - class: model
              l1: 'Yes'
              l2: panes discard elements when fired
              l3: Elements are discarded from accumulated state as their pane is fired.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: ''
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'Yes'
              l2: fully supported
              l3: 'Spark streaming natively discards elements after firing.'
              
        - name: Accumulating
          values:
            - class: model
              l1: 'Yes'
              l2: panes accumulate elements across firings
              l3: Elements are accumulated in state across multiple pane firings for the same window.
            - class: dataflow
              l1: 'Yes'
              l2: fully supported
              l3: Requires that the accumulated pane fits in memory, after being passed through the combiner (if relevant)
            - class: flink
              l1: 'Yes'
              l2: fully supported
              l3: "The Runner uses Beam's Windowing and Triggering logic and code."
            - class: spark
              l1: 'No'
              l2: ''
              l3: ''
              
        - name: 'Accumulating &amp; Retracting'
          values:
            - class: model
              jira: BEAM-91
              l1: 'No'
              l2: accumulation plus retraction of old panes
              l3: Elements are accumulated across multiple pane firings and old emitted values are retracted. Also known as "backsies" ;-D
            - class: dataflow
              l1: 'No'
              l2: pending model support
              l3: ''
            - class: flink
              l1: 'No'
              l2: pending model support
              l3: ''
            - class: spark
              l1: 'No'
              l2: pending model support
              l3: ''
              

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

With initial code drops complete ([Dataflow SDK and Runner](https://github.com/apache/beam/pull/1), [Flink Runner](https://github.com/apache/beam/pull/12), [Spark Runner](https://github.com/apache/beam/pull/42)) and expressed interest in runner implementations for [Storm](https://issues.apache.org/jira/browse/BEAM-9), [Hadoop](https://issues.apache.org/jira/browse/BEAM-19), and [Gearpump](https://issues.apache.org/jira/browse/BEAM-79) (amongst others), we wanted to start addressing a big question in the Apache Beam (incubating) community: what capabilities will each runner be able to support?

<!--more-->

While we’d love to have a world where all runners support the full suite of semantics included in the Beam Model (formerly referred to as the [Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)), practically speaking, there will always be certain features that some runners can’t provide. For example, a Hadoop-based runner would be inherently batch-based and may be unable to (easily) implement support for unbounded collections. However, that doesn’t prevent it from being extremely useful for a large set of uses. In other cases, the implementations provided by one runner may have slightly different semantics that those provided by another (e.g. even though the current suite of runners all support exactly-once delivery guarantees, an [Apache Samza](https://samza.apache.org/) runner, which would be a welcome addition, would currently only support at-least-once).

To help clarify things, we’ve been working on enumerating the key features of the Beam model in a [capability matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) for all existing runners, categorized around the four key questions addressed by the model: <span class="wwwh-what-dark">What</span> / <span class="wwwh-where-dark">Where</span> / <span class="wwwh-when-dark">When</span> / <span class="wwwh-how-dark">How</span> (if you’re not familiar with those questions, you might want to read through [Streaming 102](https://oreilly.com/ideas/the-world-beyond-batch-streaming-102) for an overview). This table will be maintained over time as the model evolves, our understanding grows, and runners are created or features added.

Included below is a summary snapshot of our current understanding of the capabilities of the existing runners (see the [live version]({{ site.baseurl }}/documentation/runners/capability-matrix/) for full details, descriptions, and Jira links); since integration is still under way, the system as whole isn’t yet in a completely stable, usable state. But that should be changing in the near future, and we’ll be updating loud and clear on this blog when the first supported Beam 1.0 release happens.

In the meantime, these tables should help clarify where we expect to be in the very near term, and help guide expectations about what existing runners are capable of, and what features runner implementers will be tackling next.

{% include capability-matrix-common.md %}
{% assign cap-data=page.capability-matrix-snapshot %}

<!-- Summary table -->
{% assign cap-style='cap-summary' %}
{% assign cap-view='blog' %}
{% assign cap-other-view='full' %}
{% assign cap-toggle-details=1 %}
{% assign cap-display='block' %}

{% include capability-matrix.md %}
