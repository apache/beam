/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.Timestamp;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueKind} support in {@link DoFn}. */
@RunWith(JUnit4.class)
public class ValueKindTest implements Serializable {

  static {
    System.setProperty(
        "beamTestPipelineOptions", "[\"--runner=org.apache.beam.runners.direct.DirectRunner\"]");
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindParameterInDoFn() {
    PCollection<String> input = pipeline.apply(Create.of("a", "b"));

    PCollection<String> output =
        input.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element String element, ProcessContext c, ValueKind kind) {
                    c.output(element + ":" + kind);
                  }
                }));

    PAssert.that(output).containsInAnyOrder("a:INSERT", "b:INSERT");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputWithKind() {
    PCollection<String> input = pipeline.apply(Create.of("a", "b"));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        if ("a".equals(element)) {
                          c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                        } else {
                          c.outputWithKind(element, ValueKind.UPDATE_AFTER);
                        }
                      }
                    }))
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE", "b:UPDATE_AFTER");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDefaultValueKind() {
    PCollection<String> input = pipeline.apply(Create.of("a"));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(
                "StandardOutput",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.output(element); // Should preserve input kind!
                      }
                    }))
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInOutputWindowedValue_MainOutput() {
    PCollection<String> input = pipeline.apply(Create.of("a"));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(
                "OutputWindowedValue",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, BoundedWindow window) {
                        // Should preserve input kind!
                        c.outputWindowedValue(
                            element, c.timestamp(), Collections.singleton(window), c.pane());
                      }
                    }))
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInOutputWindowedValue_TaggedOutput() {
    PCollection<String> input = pipeline.apply(Create.of("a"));
    TupleTag<String> mainTag = new TupleTag<String>() {};
    TupleTag<String> sideTag = new TupleTag<String>() {};

    PCollectionTuple outputTuple =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(
                "OutputWindowedValueTagged",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(
                              @Element String element, ProcessContext c, BoundedWindow window) {
                            c.outputWindowedValue(
                                sideTag,
                                element,
                                c.timestamp(),
                                Collections.singleton(window),
                                c.pane());
                          }
                        })
                    .withOutputTags(mainTag, TupleTagList.of(sideTag)));

    PCollection<String> output =
        outputTuple
            .get(sideTag)
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInOutputWindowedValue_Object() {
    PCollection<String> input = pipeline.apply(Create.of("a"));

    PCollection<String> output =
        input
            .apply(
                "OutputWindowedValueObject",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, BoundedWindow window) {
                        c.outputWindowedValue(
                            WindowedValues.of(
                                element,
                                c.timestamp(),
                                Collections.singleton(window),
                                c.pane(),
                                null,
                                null,
                                org.apache.beam.sdk.values.CausedByDrain.NORMAL,
                                ValueKind.UPDATE_BEFORE));
                      }
                    }))
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInOutputWindowedValue_TaggedObject() {
    PCollection<String> input = pipeline.apply(Create.of("a"));
    TupleTag<String> mainTag = new TupleTag<String>() {};
    TupleTag<String> sideTag = new TupleTag<String>() {};

    PCollectionTuple outputTuple =
        input.apply(
            "OutputWindowedValueTaggedObject",
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, BoundedWindow window) {
                        c.outputWindowedValue(
                            sideTag,
                            WindowedValues.of(
                                element,
                                c.timestamp(),
                                Collections.singleton(window),
                                c.pane(),
                                null,
                                null,
                                org.apache.beam.sdk.values.CausedByDrain.NORMAL,
                                ValueKind.UPDATE_BEFORE));
                      }
                    })
                .withOutputTags(mainTag, TupleTagList.of(sideTag)));

    PCollection<String> output =
        outputTuple
            .get(sideTag)
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedAcrossTags() {
    PCollection<String> input = pipeline.apply(Create.of("a"));
    TupleTag<String> mainTag = new TupleTag<String>() {};
    TupleTag<String> sideTag = new TupleTag<String>() {};

    PCollectionTuple outputTuple =
        input.apply(
            "OutputWithKindTagged",
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.outputWithKind(sideTag, element, ValueKind.UPDATE_BEFORE);
                      }
                    })
                .withOutputTags(mainTag, TupleTagList.of(sideTag)));

    PCollection<String> output =
        outputTuple
            .get(sideTag)
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindInSplittableDoFn() {
    PCollection<String> input = pipeline.apply(Create.of("a"));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(@Element String element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(
                "SplittableDoFn",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element,
                          RestrictionTracker<OffsetRange, Long> tracker,
                          ProcessContext c,
                          ValueKind kind) {
                        if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
                          c.output(element + ":" + kind);
                        }
                      }

                      @GetInitialRestriction
                      public OffsetRange getInitialRestriction(@Element String element) {
                        return new OffsetRange(0, 1);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("a:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputWithKindInWindowExpiration() {
    PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of("key", "a")));
    PCollection<KV<String, String>> windowedInput =
        input.apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));

    PCollection<String> output =
        windowedInput.apply(
            "StatefulParDo",
            ParDo.of(
                new DoFn<KV<String, String>, String>() {
                  @StateId("dummy")
                  private final StateSpec<ValueState<Integer>> spec =
                      StateSpecs.value(VarIntCoder.of());

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, String> element, ProcessContext c) {
                    // Do nothing, just let state be created
                  }

                  @OnWindowExpiration
                  public void onWindowExpiration(
                      OutputReceiver<String> receiver,
                      BoundedWindow window,
                      @Timestamp org.joda.time.Instant timestamp) {
                    receiver.outputWindowedValue(
                        "expired",
                        timestamp,
                        Collections.singleton(window),
                        PaneInfo.NO_FIRING,
                        ValueKind.UPDATE_BEFORE);
                  }
                }));

    PAssert.that(output).containsInAnyOrder("expired");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputWithKindInOnTimer() {
    PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of("key", "a")));

    PCollection<String> output =
        input.apply(
            "TimerParDo",
            ParDo.of(
                new DoFn<KV<String, String>, String>() {
                  @TimerId("timer")
                  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, String> element,
                      @TimerId("timer") Timer timer,
                      ProcessContext c) {
                    timer.set(c.timestamp().plus(Duration.standardSeconds(1)));
                  }

                  @OnTimer("timer")
                  public void onTimer(OnTimerContext c) {
                    c.outputWithKind("timed_out", ValueKind.UPDATE_BEFORE);
                  }
                }));

    PAssert.that(output).containsInAnyOrder("timed_out");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInReshuffle() {
    PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of("key", "value")));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<KV<String, String>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(Reshuffle.of())
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<KV<String, String>, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> element, ProcessContext c, ValueKind kind) {
                        c.output(element.getValue() + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("value:UPDATE_BEFORE");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValueKindPreservedInGroupByKeyWithReify() {
    PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of("key", "value")));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<KV<String, String>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> element, ProcessContext c) {
                        c.outputWithKind(element, ValueKind.UPDATE_BEFORE);
                      }
                    }))
            .apply(Reify.windowsInValue())
            .apply(GroupByKey.create())
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<KV<String, Iterable<ValueInSingleWindow<String>>>, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Iterable<ValueInSingleWindow<String>>> element,
                          ProcessContext c) {
                        for (ValueInSingleWindow<String> value : element.getValue()) {
                          c.output(value.getValue() + ":" + value.getValueKind());
                        }
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("value:UPDATE_BEFORE");
    pipeline.run();
  }
}
