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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RerunTest;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
 * Tests for (multi)map {@link View}. See also {@link ParDoTest}, which provides additional coverage
 * since views can only be observed via {@link ParDo}.
 */
@RunWith(JUnit4.class)
@Category({UsesSideInputs.class, RerunTest.class})
public class MapViewTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  @Test
  @Category(ValidatesRunner.class)
  public void testMultimapSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("apple", 1),
            KV.of("apple", 1),
            KV.of("apple", 2),
            KV.of("banana", 3),
            KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMultimapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of(2 /* size */))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertEquals((int) c.element(), c.sideInput(view).size());
                            assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                            for (Entry<String, Iterable<Integer>> entry :
                                c.sideInput(view).entrySet()) {
                              for (Integer value : entry.getValue()) {
                                c.output(KV.of(entry.getKey(), value));
                              }
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultimapInMemorySideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.<String, Integer>asMultimap().inMemory());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("apple", 1),
            KV.of("apple", 1),
            KV.of("apple", 2),
            KV.of("banana", 3),
            KV.of("blackberry", 3));

    pipeline.run();
  }

  private static class NonDeterministicStringCoder extends AtomicCoder<String> {
    @Override
    public void encode(String value, OutputStream outStream) throws CoderException, IOException {
      encode(value, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(String value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value, outStream, context);
    }

    @Override
    public String decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public String decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      return StringUtf8Coder.of().decode(inStream, context);
    }

    @Override
    public void verifyDeterministic()
        throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
      throw new NonDeterministicException(this, "Test coder is not deterministic on purpose.");
    }
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testMultimapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("apple", 1),
            KV.of("apple", 1),
            KV.of("apple", 2),
            KV.of("banana", 3),
            KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedMultimapSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                    TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(14))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("apple", new Instant(5)),
                    TimestampedValue.of("banana", new Instant(13)),
                    TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("apple", 1),
            KV.of("apple", 1),
            KV.of("apple", 2),
            KV.of("banana", 3),
            KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedMultimapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                    TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(14))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of(1 /* size */, new Instant(5)),
                    TimestampedValue.of(1 /* size */, new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertEquals((int) c.element(), c.sideInput(view).size());
                            assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                            for (Entry<String, Iterable<Integer>> entry :
                                c.sideInput(view).entrySet()) {
                              for (Integer value : entry.getValue()) {
                                c.output(KV.of(entry.getKey(), value));
                              }
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("a", 1), KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testWindowedMultimapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                        TimestampedValue.of(KV.of("a", 1), new Instant(2)),
                        TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                        TimestampedValue.of(KV.of("b", 3), new Instant(14)))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("apple", new Instant(5)),
                    TimestampedValue.of("banana", new Instant(13)),
                    TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("apple", 1),
            KV.of("apple", 1),
            KV.of("apple", 2),
            KV.of("banana", 3),
            KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmptyMultimapSideInput() throws Exception {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateEmptyView", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply(View.asMultimap());

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
                            assertTrue(c.sideInput(view).entrySet().isEmpty());
                            assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                            c.output(c.element());
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testEmptyMultimapSideInputWithNonDeterministicKeyCoder() throws Exception {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateEmptyView",
                Create.empty(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.asMultimap());

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
                            assertTrue(c.sideInput(view).entrySet().isEmpty());
                            assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                            c.output(c.element());
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMultimapSideInputIsImmutable() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1))).apply(View.asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            try {
                              c.sideInput(view).clear();
                              fail("Expected UnsupportedOperationException on clear()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).put("c", ImmutableList.of(3));
                              fail("Expected UnsupportedOperationException on put()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).remove("c");
                              fail("Expected UnsupportedOperationException on remove()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).putAll(new HashMap<>());
                              fail("Expected UnsupportedOperationException on putAll()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                              c.output(KV.of(c.element(), v));
                            }
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of(2 /* size */))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertEquals((int) c.element(), c.sideInput(view).size());
                            assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                            for (Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                              c.output(KV.of(entry.getKey(), entry.getValue()));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(KV.of("a", 1), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapInMemorySideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.<String, Integer>asMap().inMemory());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapInMemorySideInputWithNonStructuralKey() {

    final PCollectionView<Map<byte[], Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(
                    KV.of("a".getBytes(StandardCharsets.UTF_8), 1),
                    KV.of("b".getBytes(StandardCharsets.UTF_8), 3)))
            .apply(View.<byte[], Integer>asMap().inMemory());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view)
                                        .get(
                                            c.element()
                                                .substring(0, 1)
                                                .getBytes(StandardCharsets.UTF_8))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testMapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("b", 3))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(18))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("apple", new Instant(5)),
                    TimestampedValue.of("banana", new Instant(4)),
                    TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedMapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(18))))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of(2 /* size */, new Instant(5)),
                    TimestampedValue.of(1 /* size */, new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            assertEquals((int) c.element(), c.sideInput(view).size());
                            assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                            for (Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                              c.output(KV.of(entry.getKey(), entry.getValue()));
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).containsInAnyOrder(KV.of("a", 1), KV.of("b", 2), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testWindowedMapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                        TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                        TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                        TimestampedValue.of(KV.of("b", 3), new Instant(18)))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply("SideWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of("apple", new Instant(5)),
                    TimestampedValue.of("banana", new Instant(4)),
                    TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmptyMapSideInput() throws Exception {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateEmptyView", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply(View.asMap());

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
                            assertTrue(c.sideInput(view).entrySet().isEmpty());
                            assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                            c.output(c.element());
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testEmptyMapSideInputWithNonDeterministicKeyCoder() throws Exception {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateEmptyView",
                Create.empty(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.asMap());

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
                            assertTrue(c.sideInput(view).entrySet().isEmpty());
                            assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                            c.output(c.element());
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMapSideInputWithNullValuesCatchesDuplicates() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", (Integer) null), KV.of("a", (Integer) null))
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), NullableCoder.of(VarIntCoder.of()))))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view)
                                        .getOrDefault(c.element().substring(0, 1), 0)));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    // As long as we get an error, be flexible with how a runner surfaces it
    thrown.expect(Exception.class);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMapSideInputIsImmutable() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1))).apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple"))
            .apply(
                "OutputSideInputs",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            try {
                              c.sideInput(view).clear();
                              fail("Expected UnsupportedOperationException on clear()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).put("c", 3);
                              fail("Expected UnsupportedOperationException on put()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).remove("c");
                              fail("Expected UnsupportedOperationException on remove()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            try {
                              c.sideInput(view).putAll(new HashMap<>());
                              fail("Expected UnsupportedOperationException on putAll()");
                            } catch (UnsupportedOperationException expected) {
                            }
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombinedMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 20), KV.of("b", 3)))
            .apply("SumIntegers", Combine.perKey(Sum.ofIntegers()))
            .apply(View.asMap());

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "Output",
                ParDo.of(
                        new DoFn<String, KV<String, Integer>>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(
                                KV.of(
                                    c.element(),
                                    c.sideInput(view).get(c.element().substring(0, 1))));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(KV.of("apple", 21), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }
}
