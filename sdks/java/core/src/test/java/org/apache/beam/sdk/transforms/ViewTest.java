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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RerunTest;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTriggeredSideInputs;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link View}. See also {@link ParDoTest}, which provides additional coverage since
 * views can only be observed via {@link ParDo}.
 */
@RunWith(JUnit4.class)
@Category({UsesSideInputs.class, RerunTest.class})
public class ViewTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  @Test
  @Category(ValidatesRunner.class)
  public void testSingletonSideInput() {

    final PCollectionView<Integer> view =
        pipeline.apply("Create47", Create.of(47)).apply(View.asSingleton());

    PCollection<Integer> output =
        pipeline
            .apply("Create123", Create.of(1, 2, 3))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(47, 47, 47);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedSingletonSideInput() {

    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "Create47",
                Create.timestamped(
                    TimestampedValue.of(47, new Instant(1)),
                    TimestampedValue.of(48, new Instant(11))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asSingleton());

    PCollection<Integer> output =
        pipeline
            .apply(
                "Create123",
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(4)),
                    TimestampedValue.of(2, new Instant(8)),
                    TimestampedValue.of(3, new Instant(12))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(47, 47, 48);

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testWindowedSideInputNotPresent() {
    PCollection<KV<Long, Long>> input =
        pipeline.apply(
            TestStream.create(KvCoder.of(VarLongCoder.of(), VarLongCoder.of()))
                .advanceWatermarkTo(new Instant(0))
                .addElements(TimestampedValue.of(KV.of(1000L, 1000L), new Instant(1000L)))
                .advanceWatermarkTo(new Instant(20000))
                .advanceWatermarkToInfinity());

    final PCollectionView<Long> view =
        input
            .apply(Values.create())
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.standardSeconds(100))))
            .apply("ViewCombine", Combine.globally(Sum.ofLongs()).withoutDefaults())
            .apply("Rewindow", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(View.<Long>asSingleton().withDefaultValue(0L));

    PCollection<Long> output =
        input
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(GroupByKey.create())
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<KV<Long, Iterable<Long>>, Long>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .inWindow(new IntervalWindow(new Instant(0), new Instant(10000)))
        .containsInAnyOrder(0L);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmptySingletonSideInput() throws Exception {

    final PCollectionView<Integer> view =
        pipeline
            .apply("CreateEmptyIntegers", Create.empty(VarIntCoder.of()))
            .apply(View.asSingleton());

    pipeline
        .apply("Create123", Create.of(1, 2, 3))
        .apply(
            "OutputSideInputs",
            ParDo.of(
                    new DoFn<Integer, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(c.sideInput(view));
                      }
                    })
                .withSideInputs(view));

    // As long as we get an error, be flexible with how a runner surfaces it
    thrown.expect(Exception.class);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testNonSingletonSideInput() throws Exception {

    PCollection<Integer> oneTwoThree = pipeline.apply(Create.of(1, 2, 3));
    final PCollectionView<Integer> view = oneTwoThree.apply(View.asSingleton());

    oneTwoThree.apply(
        "OutputSideInputs",
        ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                })
            .withSideInputs(view));

    // As long as we get an error, be flexible with how a runner surfaces it
    thrown.expect(Exception.class);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testDiscardingNonSingletonSideInput() throws Exception {

    PCollection<Integer> oneTwoThree = pipeline.apply(Create.of(1, 2, 3));
    final PCollectionView<Integer> view =
        oneTwoThree
            .apply(Window.<Integer>configure().discardingFiredPanes())
            .apply(View.asSingleton());

    oneTwoThree.apply(
        "OutputSideInputs",
        ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                })
            .withSideInputs(view));

    // As long as we get an error, be flexible with how a runner surfaces it
    thrown.expect(Exception.class);

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesTriggeredSideInputs.class})
  public void testTriggeredLatestSingleton() {
    IntervalWindow zeroWindow = new IntervalWindow(new Instant(0), new Instant(1000));

    PCollectionView<Long> view =
        pipeline
            .apply(
                GenerateSequence.from(0)
                    .withRate(1, Duration.millis(100))
                    .withTimestampFn(Instant::new)
                    .withMaxReadTime(Duration.standardSeconds(10)))
            .apply(
                "Window side input",
                Window.<Long>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(Reify.timestamps())
            .apply(Combine.globally(Latest.<Long>combineFn()).withoutDefaults().asSingletonView());

    final String tag = "singleton";
    PCollection<Long> pc =
        pipeline
            .apply(Impulse.create())
            .apply(WithTimestamps.of(impulse -> new Instant(0)))
            .apply("Window main input", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
            .apply(
                ParDo.of(
                        new DoFn<byte[], Long>() {
                          @ProcessElement
                          public void process(
                              @SideInput(tag) Long sideInput, OutputReceiver<Long> out)
                              throws InterruptedException {
                            // waiting to ensure multiple outputs to side input before reading it
                            Thread.sleep(1000L);
                            out.output(sideInput);
                          }
                        })
                    .withSideInput(tag, view));

    PAssert.that(pc)
        .inWindow(zeroWindow)
        .satisfies(
            (Iterable<Long> values) -> {
              assertThat(values, Matchers.iterableWithSize(1));
              return null;
            });

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testListSideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23)).apply(View.asList());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29, 31))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            checkArgument(c.sideInput(view).size() == 4);
                            checkArgument(
                                c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testListWithRandomAccessSideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(11, 13, 17, 23))
            .apply(View.<Integer>asList().withRandomAccess());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29, 31))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            checkArgument(c.sideInput(view).size() == 4);
                            checkArgument(
                                c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testListInMemorySideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(11, 13, 17, 23))
            .apply(View.<Integer>asList().inMemory());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29, 31))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            checkArgument(c.sideInput(view).size() == 4);
                            checkArgument(
                                c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedListSideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(11, new Instant(1)),
                    TimestampedValue.of(13, new Instant(1)),
                    TimestampedValue.of(17, new Instant(1)),
                    TimestampedValue.of(23, new Instant(1)),
                    TimestampedValue.of(31, new Instant(11)),
                    TimestampedValue.of(33, new Instant(11)),
                    TimestampedValue.of(37, new Instant(11)),
                    TimestampedValue.of(43, new Instant(11))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asList());

    PCollection<Integer> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of(29, new Instant(1)),
                    TimestampedValue.of(35, new Instant(11))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            checkArgument(c.sideInput(view).size() == 4);
                            checkArgument(
                                c.sideInput(view).get(0).equals(c.sideInput(view).get(0)));
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmptyListSideInput() throws Exception {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of())).apply(View.asList());

    PCollection<Integer> results =
        pipeline
            .apply("Create1", Create.of(1))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertTrue(c.sideInput(view).isEmpty());
                            assertFalse(c.sideInput(view).iterator().hasNext());
                            c.output(1);
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testListSideInputIsImmutable() {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11)).apply(View.asList());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            try {
                              c.sideInput(view).clear();
                              fail("Expected UnsupportedOperationException on clear()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).add(4);
                              fail("Expected UnsupportedOperationException on add()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).addAll(new ArrayList<>());
                              fail("Expected UnsupportedOperationException on addAll()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).remove(0);
                              fail("Expected UnsupportedOperationException on remove()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(11);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testIterableSideInput() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23)).apply(View.asIterable());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29, 31))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedIterableSideInput() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(11, new Instant(1)),
                    TimestampedValue.of(13, new Instant(1)),
                    TimestampedValue.of(17, new Instant(1)),
                    TimestampedValue.of(23, new Instant(1)),
                    TimestampedValue.of(31, new Instant(11)),
                    TimestampedValue.of(33, new Instant(11)),
                    TimestampedValue.of(37, new Instant(11)),
                    TimestampedValue.of(43, new Instant(11))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asIterable());

    PCollection<Integer> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of(29, new Instant(1)),
                    TimestampedValue.of(35, new Instant(11))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer i : c.sideInput(view)) {
                              c.output(i);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmptyIterableSideInput() throws Exception {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of())).apply(View.asIterable());

    PCollection<Integer> results =
        pipeline
            .apply("Create1", Create.of(1))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertFalse(c.sideInput(view).iterator().hasNext());
                            c.output(1);
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testIterableSideInputIsImmutable() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11)).apply(View.asIterable());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput", Create.of(29))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            Iterator<Integer> iterator = c.sideInput(view).iterator();
                            while (iterator.hasNext()) {
                              try {
                                iterator.remove();
                                fail("Expected UnsupportedOperationException on remove()");
                              } catch (UnsupportedOperationException expected) {
                              }
                              c.output(iterator.next());
                            }
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(11);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedSideInputFixedToFixed() {

    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(11)),
                    TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersGlobally().withoutDefaults())
            .apply(View.asSingleton());

    PCollection<String> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("A", new Instant(4)),
                    TimestampedValue.of("B", new Instant(15)),
                    TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputMainAndSideInputs",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.element() + c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder("A1", "B5", "C1");

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedSideInputFixedToGlobal() {

    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(11)),
                    TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.into(new GlobalWindows()))
            .apply(Sum.integersGlobally())
            .apply(View.asSingleton());

    PCollection<String> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("A", new Instant(4)),
                    TimestampedValue.of("B", new Instant(15)),
                    TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputMainAndSideInputs",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.element() + c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder("A6", "B6", "C6");

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedSideInputFixedToFixedWithDefault() {

    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(2, new Instant(11)),
                    TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersGlobally().asSingletonView());

    PCollection<String> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("A", new Instant(4)),
                    TimestampedValue.of("B", new Instant(15)),
                    TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputMainAndSideInputs",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.element() + c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder("A0", "B5", "C0");

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSideInputWithNullDefault() {

    final PCollectionView<Void> view =
        pipeline
            .apply("CreateSideInput", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(Combine.globally((Iterable<Void> input) -> null).asSingletonView());

    @SuppressWarnings("ObjectToString")
    PCollection<String> output =
        pipeline
            .apply("CreateMainInput", Create.of(""))
            .apply(
                "OutputMainAndSideInputs",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.element() + c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder("null");

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSideInputWithNestedIterables() {
    final PCollectionView<Iterable<Integer>> view1 =
        pipeline
            .apply("CreateVoid1", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(
                "OutputOneInteger",
                ParDo.of(
                    new DoFn<Void, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(17);
                      }
                    }))
            .apply("View1", View.asIterable());

    final PCollectionView<Iterable<Iterable<Integer>>> view2 =
        pipeline
            .apply("CreateVoid2", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(
                "OutputSideInput",
                ParDo.of(
                        new DoFn<Void, Iterable<Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view1));
                          }
                        })
                    .withSideInputs(view1))
            .apply("View2", View.asIterable());

    PCollection<Integer> output =
        pipeline
            .apply("CreateVoid3", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(
                "ReadIterableSideInput",
                ParDo.of(
                        new DoFn<Void, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Iterable<Integer> input : c.sideInput(view2)) {
                              for (Integer i : input) {
                                c.output(i);
                              }
                            }
                          }
                        })
                    .withSideInputs(view2));

    PAssert.that(output).containsInAnyOrder(17);

    pipeline.run();
  }
}
