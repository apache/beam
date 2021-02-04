---
title:  "Testing Unbounded Pipelines in Apache Beam"
date:   2016-10-20 10:00:00 -0800
categories:
  - blog
aliases:
  - /blog/2016/10/20/test-stream.html
authors:
  - tgroh
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

The Beam Programming Model unifies writing pipelines for Batch and Streaming
pipelines. We’ve recently introduced a new PTransform to write tests for
pipelines that will be run over unbounded datasets and must handle out-of-order
and delayed data.
<!--more-->

Watermarks, Windows and Triggers form a core part of the Beam programming model
-- they respectively determine how your data are grouped, when your input is
complete, and when to produce results. This is true for all pipelines,
regardless of if they are processing bounded or unbounded inputs. If you’re not
familiar with watermarks, windowing, and triggering in the Beam model,
[Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101)
and [Streaming 102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102)
are an excellent place to get started. A key takeaway from
these articles: in realistic streaming scenarios with intermittent failures and
disconnected users, data can arrive out of order or be delayed. Beam’s
primitives provide a way for users to perform useful, powerful, and correct
computations in spite of these challenges.

As Beam pipeline authors, we need comprehensive tests that cover crucial
failure scenarios and corner cases to gain real confidence that a pipeline is
ready for production. The existing testing infrastructure within the Beam SDKs
permits tests to be written which examine the contents of a Pipeline at
execution time. However, writing unit tests for pipelines that may receive
late data or trigger multiple times has historically ranged from complex to
not possible, as pipelines that read from unbounded sources do not shut down
without external intervention, while pipelines that read from bounded sources
exclusively cannot test behavior with late data nor most speculative triggers.
Without additional tools, pipelines that use custom triggers and handle
out-of-order data could not be easily tested.

This blog post introduces our new framework for writing tests for pipelines that
handle delayed and out-of-order data in the context of the LeaderBoard pipeline
from the Mobile Gaming example series.

## LeaderBoard and the Mobile Gaming Example

[LeaderBoard](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java#L177)
is part of the [Beam mobile gaming examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete/game)
(and [walkthroughs](/get-started/mobile-gaming-example/))
which produces a continuous accounting of user and team scores. User scores are
calculated over the lifetime of the program, while team scores are calculated
within fixed windows with a default duration of one hour. The LeaderBoard
pipeline produces speculative and late panes as appropriate, based on the
configured triggering and allowed lateness of the pipeline. The expected outputs
of the LeaderBoard pipeline vary depending on when elements arrive in relation
to the watermark and the progress of processing time, which could not previously
be controlled within a test.

## Writing Deterministic Tests to Emulate Nondeterminism

The Beam testing infrastructure provides the
[PAssert](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/testing/PAssert.html)
methods, which assert properties about the contents of a PCollection from within
a pipeline. We have expanded this infrastructure to include
[TestStream](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/TestStream.java),
which is a PTransform that performs a series of events, consisting of adding
additional elements to a pipeline, advancing the watermark of the TestStream,
and advancing the pipeline processing time clock. TestStream permits tests which
observe the effects of triggers on the output a pipeline produces.

While executing a pipeline that reads from a TestStream, the read waits for all
of the consequences of each event to complete before continuing on to the next
event, ensuring that when processing time advances, triggers that are based on
processing time fire as appropriate. With this transform, the effect of
triggering and allowed lateness can be observed on a pipeline, including
reactions to speculative and late panes and dropped data.

## Element Timings

Elements arrive either before, with, or after the watermark, which categorizes
them into the "early", "on-time", and "late" divisions. "Late" elements can be
further subdivided into "unobservably", "observably", and "droppably" late,
depending on the window to which they are assigned and the maximum allowed
lateness, as specified by the windowing strategy. Elements that arrive with
these timings are emitted into panes, which can be "EARLY", "ON-TIME", or
"LATE", depending on the position of the watermark when the pane was emitted.

Using TestStream, we can write tests that demonstrate that speculative panes are
output after their trigger condition is met, that the advancing of the watermark
causes the on-time pane to be produced, and that late-arriving data produces
refinements when it arrives before the maximum allowed lateness, and is dropped
after.

The following examples demonstrate how you can use TestStream to provide a
sequence of events to the Pipeline, where the arrival of elements is interspersed
with updates to the watermark and the advance of processing time. Each of these
events runs to completion before additional events occur.

In the diagrams, the time at which events occurred in "real" (event) time
progresses as the graph moves to the right. The time at which the pipeline
receives them progresses as the graph goes upwards. The watermark is represented
by the squiggly red line, and each starburst is the firing of a trigger and the
associated pane.

<img class="center-block" src="/images/blog/test-stream/elements-all-on-time.png" alt="Elements on the Event and Processing time axes, with the Watermark and produced panes" width="442">

### Everything arrives on-time

For example, if we create a TestStream where all the data arrives before the
watermark and provide the result PCollection as input to the CalculateTeamScores
PTransform:

{{< highlight java >}}
TestStream<GameActionInfo> infos = TestStream.create(AvroCoder.of(GameActionInfo.class))
    .addElements(new GameActionInfo("sky", "blue", 12, new Instant(0L)),
                 new GameActionInfo("navy", "blue", 3, new Instant(0L)),
                 new GameActionInfo("navy", "blue", 3, new Instant(0L).plus(Duration.standardMinutes(3))))
    // Move the watermark past the end the end of the window
    .advanceWatermarkTo(new Instant(0L).plus(TEAM_WINDOW_DURATION)
                                       .plus(Duration.standardMinutes(1)))
    .advanceWatermarkToInfinity();

PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
    .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));
{{< /highlight >}}

we can then assert that the result PCollection contains elements that arrived:

<img class="center-block" src="/images/blog/test-stream/elements-all-on-time.png" alt="Elements all arrive before the watermark, and are produced in the on-time pane" width="442">

{{< highlight java >}}
// Only one value is emitted for the blue team
PAssert.that(teamScores)
       .inWindow(window)
       .containsInAnyOrder(KV.of("blue", 18));
p.run();
{{< /highlight >}}

### Some elements are late, but arrive before the end of the window

We can also add data to the TestStream after the watermark, but before the end
of the window (shown below to the left of the red watermark), which demonstrates
"unobservably late" data - that is, data that arrives late, but is promoted by
the system to be on time, as it arrives before the watermark passes the end of
the window

{{< highlight java >}}
TestStream<GameActionInfo> infos = TestStream.create(AvroCoder.of(GameActionInfo.class))
    .addElements(new GameActionInfo("sky", "blue", 3, new Instant(0L)),
                 new GameActionInfo("navy", "blue", 3, new Instant(0L).plus(Duration.standardMinutes(3))))
    // Move the watermark up to "near" the end of the window
    .advanceWatermarkTo(new Instant(0L).plus(TEAM_WINDOW_DURATION)
                                       .minus(Duration.standardMinutes(1)))
    .addElements(new GameActionInfo("sky", "blue", 12, Duration.ZERO))
    .advanceWatermarkToInfinity();

PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
    .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));
{{< /highlight >}}

<img class="center-block" src="/images/blog/test-stream/elements-unobservably-late.png" alt="An element arrives late, but before the watermark passes the end of the window, and is produced in the on-time pane" width="442">

{{< highlight java >}}
// Only one value is emitted for the blue team
PAssert.that(teamScores)
       .inWindow(window)
       .containsInAnyOrder(KV.of("blue", 18));
p.run();
{{< /highlight >}}

### Elements are late, and arrive after the end of the window

By advancing the watermark farther in time before adding the late data, we can
demonstrate the triggering behavior that causes the system to emit an on-time
pane, and then after the late data arrives, a pane that refines the result.

{{< highlight java >}}
TestStream<GameActionInfo> infos = TestStream.create(AvroCoder.of(GameActionInfo.class))
    .addElements(new GameActionInfo("sky", "blue", 3, new Instant(0L)),
                 new GameActionInfo("navy", "blue", 3, new Instant(0L).plus(Duration.standardMinutes(3))))
    // Move the watermark up to "near" the end of the window
    .advanceWatermarkTo(new Instant(0L).plus(TEAM_WINDOW_DURATION)
                                       .minus(Duration.standardMinutes(1)))
    .addElements(new GameActionInfo("sky", "blue", 12, Duration.ZERO))
    .advanceWatermarkToInfinity();

PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
    .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));
{{< /highlight >}}

<img class="center-block" src="/images/blog/test-stream/elements-observably-late.png" alt="Elements all arrive before the watermark, and are produced in the on-time pane" width="442">

{{< highlight java >}}
// An on-time pane is emitted with the events that arrived before the window closed
PAssert.that(teamScores)
       .inOnTimePane(window)
       .containsInAnyOrder(KV.of("blue", 6));
// The final pane contains the late refinement
PAssert.that(teamScores)
       .inFinalPane(window)
       .containsInAnyOrder(KV.of("blue", 18));
p.run();
{{< /highlight >}}

### Elements are late, and after the end of the window plus the allowed lateness

If we push the watermark even further into the future, beyond the maximum
configured allowed lateness, we can demonstrate that the late element is dropped
by the system.

{{< highlight java >}}
TestStream<GameActionInfo> infos = TestStream.create(AvroCoder.of(GameActionInfo.class))
    .addElements(new GameActionInfo("sky", "blue", 3, Duration.ZERO),
                 new GameActionInfo("navy", "blue", 3, Duration.standardMinutes(3)))
    // Move the watermark up to "near" the end of the window
    .advanceWatermarkTo(new Instant(0).plus(TEAM_WINDOW_DURATION)
                                         .plus(ALLOWED_LATENESS)
                                         .plus(Duration.standardMinutes(1)))
    .addElements(new GameActionInfo(
                     "sky",
                     "blue",
                     12,
                     new Instant(0).plus(TEAM_WINDOW_DURATION).minus(Duration.standardMinutes(1))))
    .advanceWatermarkToInfinity();

PCollection<KV<String, Integer>> teamScores = p.apply(createEvents)
    .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));
{{< /highlight >}}

<img class="center-block" src="/images/blog/test-stream/elements-droppably-late.png" alt="Elements all arrive before the watermark, and are produced in the on-time pane" width="442">

{{< highlight java >}}
// An on-time pane is emitted with the events that arrived before the window closed
PAssert.that(teamScores)
       .inWindow(window)
       .containsInAnyOrder(KV.of("blue", 6));

p.run();
{{< /highlight >}}

### Elements arrive before the end of the window, and some processing time passes
Using additional methods, we can demonstrate the behavior of speculative
triggers by advancing the processing time of the TestStream. If we add elements
to an input PCollection, occasionally advancing the processing time clock, and
apply `CalculateUserScores`

{{< highlight java >}}
TestStream.create(AvroCoder.of(GameActionInfo.class))
    .addElements(new GameActionInfo("scarlet", "red", 3, new Instant(0L)),
                 new GameActionInfo("scarlet", "red", 2, new Instant(0L).plus(Duration.standardMinutes(1))))
    .advanceProcessingTime(Duration.standardMinutes(12))
    .addElements(new GameActionInfo("oxblood", "red", 2, new Instant(0L)).plus(Duration.standardSeconds(22)),
                 new GameActionInfo("scarlet", "red", 4, new Instant(0L).plus(Duration.standardMinutes(2))))
    .advanceProcessingTime(Duration.standardMinutes(15))
    .advanceWatermarkToInfinity();

PCollection<KV<String, Integer>> userScores =
    p.apply(infos).apply(new CalculateUserScores(ALLOWED_LATENESS));
{{< /highlight >}}

<img class="center-block" src="/images/blog/test-stream/elements-processing-speculative.png" alt="Elements all arrive before the watermark, and are produced in the on-time pane" width="442">

{{< highlight java >}}
PAssert.that(userScores)
       .inEarlyGlobalWindowPanes()
       .containsInAnyOrder(KV.of("scarlet", 5),
                           KV.of("scarlet", 9),
                           KV.of("oxblood", 2));

p.run();
{{< /highlight >}}

## TestStream - Under the Hood

TestStream relies on a pipeline concept we’ve introduced, called quiescence, to
utilize the existing runner infrastructure while providing guarantees about when
a root transform will called by the runner. This consists of properties about
pending elements and triggers, namely:

* No trigger is permitted to fire but has not fired
* All elements are either buffered in state or cannot progress until a side input becomes available

Simplified, this means that, in the absence of an advancement in input
watermarks or processing time, or additional elements being added to the
pipeline, the pipeline will not make progress. Whenever the TestStream PTransform
performs an action, the runner must not reinvoke the same instance until the
pipeline has quiesced. This ensures that the events specified by TestStream
happen "in-order", which ensures that input watermarks and the system clock do
not advance ahead of the elements they hoped to hold up.

The DirectRunner has been modified to use quiescence as the signal that it
should add more work to the Pipeline, and the implementation of TestStream in
that runner uses this fact to perform a single output per event. The DirectRunner
implementation also directly controls the runner’s system clock, ensuring that
tests will complete promptly even if there is a multi-minute processing time
trigger located within the pipeline.

The TestStream transform is supported in the DirectRunner. For most users, tests
written using TestPipeline and PAsserts will automatically function while using
TestStream.

## Summary

The addition of TestStream alongside window and pane-specific matchers in PAssert
has enabled the testing of Pipelines which produce speculative and late panes.
This permits tests for all styles of pipeline to be expressed directly within the
Java SDK. If you have questions or comments, we’d love to hear them on the
[mailing lists](/get-started/support/).
