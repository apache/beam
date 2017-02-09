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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.values.KV.of;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link View}. See also {@link ParDoTest}, which
 * provides additional coverage since views can only be
 * observed via {@link ParDo}.
 */
@RunWith(JUnit4.class)
public class ViewTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void testSingletonSideInput() {

    final PCollectionView<Integer> view =
        pipeline.apply("Create47", Create.of(47)).apply(View.<Integer>asSingleton());

    PCollection<Integer> output =
        pipeline.apply("Create123", Create.of(1, 2, 3))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(47, 47, 47);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSingletonSideInput() {

    final PCollectionView<Integer> view =
        pipeline.apply("Create47", Create.timestamped(
                                       TimestampedValue.of(47, new Instant(1)),
                                       TimestampedValue.of(48, new Instant(11))))
            .apply("SideWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<Integer>asSingleton());

    PCollection<Integer> output =
        pipeline.apply("Create123", Create.timestamped(
            TimestampedValue.of(1, new Instant(4)),
            TimestampedValue.of(2, new Instant(8)),
            TimestampedValue.of(3, new Instant(12))))
            .apply("MainWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(47, 47, 48);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEmptySingletonSideInput() throws Exception {

    final PCollectionView<Integer> view =
        pipeline.apply("CreateEmptyIntegers", Create.empty(VarIntCoder.of()))
            .apply(View.<Integer>asSingleton());

    pipeline.apply("Create123", Create.of(1, 2, 3))
        .apply("OutputSideInputs", ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.sideInput(view));
          }
        }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(NoSuchElementException.class));
    thrown.expectMessage("Empty");
    thrown.expectMessage("PCollection");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNonSingletonSideInput() throws Exception {

    PCollection<Integer> oneTwoThree = pipeline.apply(Create.<Integer>of(1, 2, 3));
    final PCollectionView<Integer> view = oneTwoThree.apply(View.<Integer>asSingleton());

    oneTwoThree.apply(
        "OutputSideInputs", ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.sideInput(view));
          }
        }));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("PCollection");
    thrown.expectMessage("more than one");
    thrown.expectMessage("singleton");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testListSideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23)).apply(View.<Integer>asList());

    PCollection<Integer> output =
        pipeline.apply("CreateMainInput", Create.of(29, 31))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    checkArgument(c.sideInput(view).size() == 4);
                    checkArgument(c.sideInput(view).get(0) == c.sideInput(view).get(0));
                    for (Integer i : c.sideInput(view)) {
                      c.output(i);
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedListSideInput() {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(11, new Instant(1)),
                                              TimestampedValue.of(13, new Instant(1)),
                                              TimestampedValue.of(17, new Instant(1)),
                                              TimestampedValue.of(23, new Instant(1)),
                                              TimestampedValue.of(31, new Instant(11)),
                                              TimestampedValue.of(33, new Instant(11)),
                                              TimestampedValue.of(37, new Instant(11)),
                                              TimestampedValue.of(43, new Instant(11))))
            .apply("SideWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<Integer>asList());

    PCollection<Integer> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
            TimestampedValue.of(29, new Instant(1)),
            TimestampedValue.of(35, new Instant(11))))
            .apply("MainWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    checkArgument(c.sideInput(view).size() == 4);
                    checkArgument(c.sideInput(view).get(0) == c.sideInput(view).get(0));
                    for (Integer i : c.sideInput(view)) {
                      c.output(i);
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyListSideInput() throws Exception {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of()))
            .apply(View.<Integer>asList());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertTrue(c.sideInput(view).isEmpty());
                    assertFalse(c.sideInput(view).iterator().hasNext());
                    c.output(1);
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testListSideInputIsImmutable() {

    final PCollectionView<List<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11)).apply(View.<Integer>asList());

    PCollection<Integer> output =
        pipeline.apply("CreateMainInput", Create.of(29))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
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
                      c.sideInput(view).addAll(new ArrayList<Integer>());
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
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(11);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testIterableSideInput() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11, 13, 17, 23))
            .apply(View.<Integer>asIterable());

    PCollection<Integer> output =
        pipeline.apply("CreateMainInput", Create.of(29, 31))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (Integer i : c.sideInput(view)) {
                      c.output(i);
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 11, 13, 17, 23);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedIterableSideInput() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(11, new Instant(1)),
                                              TimestampedValue.of(13, new Instant(1)),
                                              TimestampedValue.of(17, new Instant(1)),
                                              TimestampedValue.of(23, new Instant(1)),
                                              TimestampedValue.of(31, new Instant(11)),
                                              TimestampedValue.of(33, new Instant(11)),
                                              TimestampedValue.of(37, new Instant(11)),
                                              TimestampedValue.of(43, new Instant(11))))
            .apply("SideWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<Integer>asIterable());

    PCollection<Integer> output =
        pipeline
            .apply("CreateMainInput",
                Create.timestamped(
                    TimestampedValue.of(29, new Instant(1)),
                    TimestampedValue.of(35, new Instant(11))))
            .apply("MainWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (Integer i : c.sideInput(view)) {
                      c.output(i);
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(11, 13, 17, 23, 31, 33, 37, 43);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyIterableSideInput() throws Exception {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateEmptyView", Create.empty(VarIntCoder.of()))
            .apply(View.<Integer>asIterable());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertFalse(c.sideInput(view).iterator().hasNext());
                    c.output(1);
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testIterableSideInputIsImmutable() {

    final PCollectionView<Iterable<Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(11)).apply(View.<Integer>asIterable());

    PCollection<Integer> output =
        pipeline.apply("CreateMainInput", Create.of(29))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
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
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(11);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultimapSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                      c.output(of(c.element(), v));
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("apple", 2), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultimapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3)))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of(2 /* size */))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertEquals((int) c.element(), c.sideInput(view).size());
                    assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                    for (Entry<String, Iterable<Integer>> entry : c.sideInput(view).entrySet()) {
                      for (Integer value : entry.getValue()) {
                        c.output(KV.of(entry.getKey(), value));
                      }
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

    pipeline.run();
  }

  private static class NonDeterministicStringCoder extends CustomCoder<String> {
    @Override
    public void encode(String value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value, outStream, context);
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
  @Category(RunnableOnService.class)
  public void testMultimapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 3))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                      c.output(of(c.element(), v));
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("apple", 2), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMultimapSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                              TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                                              TimestampedValue.of(KV.of("b", 3), new Instant(14))))
            .apply(
                "SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of("apple", new Instant(5)),
                                              TimestampedValue.of("banana", new Instant(13)),
                                              TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<String, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               for (Integer v :
                                                   c.sideInput(view)
                                                       .get(c.element().substring(0, 1))) {
                                                 c.output(of(c.element(), v));
                                               }
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("apple", 2), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMultimapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                              TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                                              TimestampedValue.of(KV.of("b", 3), new Instant(14))))
            .apply(
                "SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of(1 /* size */, new Instant(5)),
                                              TimestampedValue.of(1 /* size */, new Instant(16))))
            .apply("MainWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<Integer, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               assertEquals((int) c.element(),
                                                   c.sideInput(view).size());
                                               assertEquals((int) c.element(),
                                                   c.sideInput(view).entrySet().size());
                                               for (Entry<String, Iterable<Integer>> entry
                                                   : c.sideInput(view).entrySet()) {
                                                 for (Integer value : entry.getValue()) {
                                                   c.output(KV.of(entry.getKey(), value));
                                                 }
                                               }
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 1), KV.of("a", 2), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMultimapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("a", 2), new Instant(7)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(14)))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply("SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of("apple", new Instant(5)),
                                              TimestampedValue.of("banana", new Instant(13)),
                                              TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<String, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               for (Integer v :
                                                   c.sideInput(view)
                                                       .get(c.element().substring(0, 1))) {
                                                 c.output(of(c.element(), v));
                                               }
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("apple", 2), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyMultimapSideInput() throws Exception {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateEmptyView", Create.empty(
                                              KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMultimap());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertTrue(c.sideInput(view).isEmpty());
                    assertTrue(c.sideInput(view).entrySet().isEmpty());
                    assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                    c.output(c.element());
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyMultimapSideInputWithNonDeterministicKeyCoder() throws Exception {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline
            .apply(
                "CreateEmptyView",
                Create.empty(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMultimap());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertTrue(c.sideInput(view).isEmpty());
                    assertTrue(c.sideInput(view).entrySet().isEmpty());
                    assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                    c.output(c.element());
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMultimapSideInputIsImmutable() {

    final PCollectionView<Map<String, Iterable<Integer>>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1)))
            .apply(View.<String, Integer>asMultimap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
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
                      c.sideInput(view).putAll(new HashMap<String, Iterable<Integer>>());
                      fail("Expected UnsupportedOperationException on putAll()");
                    } catch (UnsupportedOperationException expected) {
                    }
                    for (Integer v : c.sideInput(view).get(c.element().substring(0, 1))) {
                      c.output(KV.of(c.element(), v));
                    }
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("b", 3)))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of(2 /* size */))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertEquals((int) c.element(), c.sideInput(view).size());
                    assertEquals((int) c.element(), c.sideInput(view).entrySet().size());
                    for (Entry<String, Integer> entry : c.sideInput(view).entrySet()) {
                      c.output(KV.of(entry.getKey(), entry.getValue()));
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 1), KV.of("b", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput",
                Create.of(KV.of("a", 1), KV.of("b", 3))
                    .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                              TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                                              TimestampedValue.of(KV.of("b", 3), new Instant(18))))
            .apply(
                "SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of("apple", new Instant(5)),
                                              TimestampedValue.of("banana", new Instant(4)),
                                              TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<String, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               c.output(KV.of(
                                                   c.element(),
                                                   c.sideInput(view).get(
                                                       c.element().substring(0, 1))));
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMapAsEntrySetSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                              TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                                              TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                                              TimestampedValue.of(KV.of("b", 3), new Instant(18))))
            .apply(
                "SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of(2 /* size */, new Instant(5)),
                                              TimestampedValue.of(1 /* size */, new Instant(16))))
            .apply("MainWindowInto", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<Integer, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               assertEquals((int) c.element(),
                                                   c.sideInput(view).size());
                                               assertEquals((int) c.element(),
                                                   c.sideInput(view).entrySet().size());
                                               for (Entry<String, Integer> entry
                                                   : c.sideInput(view).entrySet()) {
                                                 c.output(KV.of(entry.getKey(), entry.getValue()));
                                               }
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 1), KV.of("b", 2), KV.of("b", 3));

    pipeline.run();
  }


  @Test
  @Category(RunnableOnService.class)
  public void testWindowedMapSideInputWithNonDeterministicKeyCoder() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("a", 1), new Instant(1)),
                    TimestampedValue.of(KV.of("b", 2), new Instant(4)),
                    TimestampedValue.of(KV.of("b", 3), new Instant(18)))
                .withCoder(KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(
                "SideWindowInto",
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                              TimestampedValue.of("apple", new Instant(5)),
                                              TimestampedValue.of("banana", new Instant(4)),
                                              TimestampedValue.of("blackberry", new Instant(16))))
            .apply("MainWindowInto", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputSideInputs", ParDo.withSideInputs(view).of(
                                           new DoFn<String, KV<String, Integer>>() {
                                             @ProcessElement
                                             public void processElement(ProcessContext c) {
                                               c.output(of(
                                                   c.element(),
                                                   c.sideInput(view).get(
                                                       c.element().substring(0, 1))));
                                             }
                                           }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("banana", 2), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyMapSideInput() throws Exception {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateEmptyView", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMap());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertTrue(c.sideInput(view).isEmpty());
                    assertTrue(c.sideInput(view).entrySet().isEmpty());
                    assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                    c.output(c.element());
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyMapSideInputWithNonDeterministicKeyCoder() throws Exception {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateEmptyView", Create.empty(
                KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of())))
            .apply(View.<String, Integer>asMap());

    PCollection<Integer> results =
        pipeline.apply("Create1", Create.of(1))
            .apply("OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    assertTrue(c.sideInput(view).isEmpty());
                    assertTrue(c.sideInput(view).entrySet().isEmpty());
                    assertFalse(c.sideInput(view).entrySet().iterator().hasNext());
                    c.output(c.element());
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(results).containsInAnyOrder(1);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapSideInputWithNullValuesCatchesDuplicates() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.of(KV.of("a", (Integer) null), KV.of("a", (Integer) null))
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), NullableCoder.of(VarIntCoder.of()))))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        KV.of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 1), KV.of("banana", 3), KV.of("blackberry", 3));

    // PipelineExecutionException is thrown with cause having a message stating that a
    // duplicate is not allowed.
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("Duplicate values for a")));
    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testMapSideInputIsImmutable() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1)))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple"))
            .apply(
                "OutputSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
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
                      c.sideInput(view).putAll(new HashMap<String, Integer>());
                      fail("Expected UnsupportedOperationException on putAll()");
                    } catch (UnsupportedOperationException expected) {
                    }
                    c.output(
                        KV.of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
                  }
                }));

    // Pass at least one value through to guarantee that DoFn executes.
    PAssert.that(output).containsInAnyOrder(KV.of("apple", 1));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCombinedMapSideInput() {

    final PCollectionView<Map<String, Integer>> view =
        pipeline.apply("CreateSideInput", Create.of(KV.of("a", 1), KV.of("a", 20), KV.of("b", 3)))
            .apply("SumIntegers", Combine.perKey(Sum.ofIntegers().<String>asKeyedFn()))
            .apply(View.<String, Integer>asMap());

    PCollection<KV<String, Integer>> output =
        pipeline.apply("CreateMainInput", Create.of("apple", "banana", "blackberry"))
            .apply("Output",
                ParDo.withSideInputs(view).of(new DoFn<String, KV<String, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(KV
                        .of(c.element(), c.sideInput(view).get(c.element().substring(0, 1))));
                  }
                }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("apple", 21), KV.of("banana", 3), KV.of("blackberry", 3));

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToFixed() {

    final PCollectionView<Integer> view =
        pipeline.apply(
             "CreateSideInput",
             Create.timestamped(TimestampedValue.of(1, new Instant(1)),
                 TimestampedValue.of(2, new Instant(11)), TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersGlobally().withoutDefaults())
            .apply(View.<Integer>asSingleton());

    PCollection<String> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                       TimestampedValue.of("A", new Instant(4)),
                                       TimestampedValue.of("B", new Instant(15)),
                                       TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
                                                  new DoFn<String, String>() {
                                                    @ProcessElement
                                                    public void processElement(ProcessContext c) {
                                                      c.output(c.element() + c.sideInput(view));
                                                    }
                                                  }));

    PAssert.that(output).containsInAnyOrder("A1", "B5", "C1");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToGlobal() {

    final PCollectionView<Integer> view =
        pipeline.apply(
             "CreateSideInput",
             Create.timestamped(TimestampedValue.of(1, new Instant(1)),
                 TimestampedValue.of(2, new Instant(11)), TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.<Integer>into(new GlobalWindows()))
            .apply(Sum.integersGlobally())
            .apply(View.<Integer>asSingleton());

    PCollection<String> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                       TimestampedValue.of("A", new Instant(4)),
                                       TimestampedValue.of("B", new Instant(15)),
                                       TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
                                                  new DoFn<String, String>() {
                                                    @ProcessElement
                                                    public void processElement(ProcessContext c) {
                                                      c.output(c.element() + c.sideInput(view));
                                                    }
                                                  }));

    PAssert.that(output).containsInAnyOrder("A6", "B6", "C6");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSideInputFixedToFixedWithDefault() {

    final PCollectionView<Integer> view =
        pipeline.apply("CreateSideInput", Create.timestamped(
                                       TimestampedValue.of(2, new Instant(11)),
                                       TimestampedValue.of(3, new Instant(13))))
            .apply("WindowSideInput", Window.<Integer>into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersGlobally().asSingletonView());

    PCollection<String> output =
        pipeline.apply("CreateMainInput", Create.timestamped(
                                       TimestampedValue.of("A", new Instant(4)),
                                       TimestampedValue.of("B", new Instant(15)),
                                       TimestampedValue.of("C", new Instant(7))))
            .apply("WindowMainInput", Window.<String>into(FixedWindows.of(Duration.millis(10))))
            .apply("OutputMainAndSideInputs", ParDo.withSideInputs(view).of(
                                                  new DoFn<String, String>() {
                                                    @ProcessElement
                                                    public void processElement(ProcessContext c) {
                                                      c.output(c.element() + c.sideInput(view));
                                                    }
                                                  }));

    PAssert.that(output).containsInAnyOrder("A0", "B5", "C0");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSideInputWithNullDefault() {

    final PCollectionView<Void> view =
        pipeline.apply("CreateSideInput", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(Combine.globally(new SerializableFunction<Iterable<Void>, Void>() {
              @Override
              public Void apply(Iterable<Void> input) {
                return null;
              }
            }).asSingletonView());

    PCollection<String> output =
        pipeline.apply("CreateMainInput", Create.of(""))
            .apply(
                "OutputMainAndSideInputs",
                ParDo.withSideInputs(view).of(new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element() + c.sideInput(view));
                  }
                }));

    PAssert.that(output).containsInAnyOrder("null");

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSideInputWithNestedIterables() {
    final PCollectionView<Iterable<Integer>> view1 =
        pipeline.apply("CreateVoid1", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply("OutputOneInteger", ParDo.of(new DoFn<Void, Integer>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(17);
              }
            }))
            .apply("View1", View.<Integer>asIterable());

    final PCollectionView<Iterable<Iterable<Integer>>> view2 =
        pipeline.apply("CreateVoid2", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(
                "OutputSideInput",
                ParDo.withSideInputs(view1).of(new DoFn<Void, Iterable<Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.sideInput(view1));
                  }
                }))
            .apply("View2", View.<Iterable<Integer>>asIterable());

    PCollection<Integer> output =
        pipeline.apply("CreateVoid3", Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply("ReadIterableSideInput",
                ParDo.withSideInputs(view2).of(new DoFn<Void, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (Iterable<Integer> input : c.sideInput(view2)) {
                      for (Integer i : input) {
                        c.output(i);
                      }
                    }
                  }
                }));

    PAssert.that(output).containsInAnyOrder(17);

    pipeline.run();
  }

  @Test
  public void testViewGetName() {
    assertEquals("View.AsSingleton", View.<Integer>asSingleton().getName());
    assertEquals("View.AsIterable", View.<Integer>asIterable().getName());
    assertEquals("View.AsMap", View.<String, Integer>asMap().getName());
    assertEquals("View.AsMultimap", View.<String, Integer>asMultimap().getName());
  }

  private void testViewUnbounded(
      Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("non-bounded PCollection")));
    pipeline
        .apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> expand(PBegin input) {
                return PCollection.<KV<String, Integer>>createPrimitiveOutputInternal(
                        input.getPipeline(),
                        WindowingStrategy.globalDefault(),
                        PCollection.IsBounded.UNBOUNDED)
                    .setTypeDescriptor(new TypeDescriptor<KV<String, Integer>>() {});
              }
            })
        .apply(view);
  }

  private void testViewNonmerging(
      Pipeline pipeline,
      PTransform<PCollection<KV<String, Integer>>, ? extends PCollectionView<?>> view) {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to create a side-input view from input");
    thrown.expectCause(
        ThrowableMessageMatcher.hasMessage(Matchers.containsString("Consumed by GroupByKey")));
    pipeline.apply(Create.<KV<String, Integer>>of(KV.of("hello", 5)))
        .apply(Window.<KV<String, Integer>>into(new InvalidWindows<>(
            "Consumed by GroupByKey", FixedWindows.of(Duration.standardHours(1)))))
        .apply(view);
  }

  @Test
  public void testViewUnboundedAsSingletonDirect() {
    testViewUnbounded(pipeline, View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewUnboundedAsIterableDirect() {
    testViewUnbounded(pipeline, View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewUnboundedAsListDirect() {
    testViewUnbounded(pipeline, View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewUnboundedAsMapDirect() {
    testViewUnbounded(pipeline, View.<String, Integer>asMap());
  }

  @Test
  public void testViewUnboundedAsMultimapDirect() {
    testViewUnbounded(pipeline, View.<String, Integer>asMultimap());
  }

  @Test
  public void testViewNonmergingAsSingletonDirect() {
    testViewNonmerging(pipeline, View.<KV<String, Integer>>asSingleton());
  }

  @Test
  public void testViewNonmergingAsIterableDirect() {
    testViewNonmerging(pipeline, View.<KV<String, Integer>>asIterable());
  }

  @Test
  public void testViewNonmergingAsListDirect() {
    testViewNonmerging(pipeline, View.<KV<String, Integer>>asList());
  }

  @Test
  public void testViewNonmergingAsMapDirect() {
    testViewNonmerging(pipeline, View.<String, Integer>asMap());
  }

  @Test
  public void testViewNonmergingAsMultimapDirect() {
    testViewNonmerging(pipeline, View.<String, Integer>asMultimap());
  }
}
