/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.TestUtils.KvMatcher.isKv;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests for GroupByKey.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByKeyTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void testGroupByKey() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),
        KV.of("k5", Integer.MAX_VALUE),
        KV.of("k5", Integer.MIN_VALUE),
        KV.of("k2", 66),
        KV.of("k1", 4),
        KV.of("k2", -33),
        KV.of("k3", 0));

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output)
        .satisfies(new AssertThatHasExpectedContentsForTestGroupByKey());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestGroupByKey
      implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>,
                                      Void> {
    @Override
    public Void apply(Iterable<KV<String, Iterable<Integer>>> actual) {
      assertThat(actual, containsInAnyOrder(
          isKv(is("k1"), containsInAnyOrder(3, 4)),
          isKv(is("k5"), containsInAnyOrder(Integer.MAX_VALUE,
                                            Integer.MIN_VALUE)),
          isKv(is("k2"), containsInAnyOrder(66, -33)),
          isKv(is("k3"), containsInAnyOrder(0))));
      return null;
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testGroupByKeyAndWindows() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),  // window [0, 5)
        KV.of("k5", Integer.MAX_VALUE), // window [0, 5)
        KV.of("k5", Integer.MIN_VALUE), // window [0, 5)
        KV.of("k2", 66), // window [0, 5)
        KV.of("k1", 4),  // window [5, 10)
        KV.of("k2", -33),  // window [5, 10)
        KV.of("k3", 0));  // window [5, 10)

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.timestamped(ungroupedPairs, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(Window.<KV<String, Integer>>into(FixedWindows.of(new Duration(5))))
             .apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output)
        .satisfies(new AssertThatHasExpectedContentsForTestGroupByKeyAndWindows());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestGroupByKeyAndWindows
      implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>,
                                      Void> {
    @Override
      public Void apply(Iterable<KV<String, Iterable<Integer>>> actual) {
      assertThat(actual, containsInAnyOrder(
          isKv(is("k1"), containsInAnyOrder(3)),
          isKv(is("k1"), containsInAnyOrder(4)),
          isKv(is("k5"), containsInAnyOrder(Integer.MAX_VALUE,
                                            Integer.MIN_VALUE)),
          isKv(is("k2"), containsInAnyOrder(66)),
          isKv(is("k2"), containsInAnyOrder(-33)),
          isKv(is("k3"), containsInAnyOrder(0))));
      return null;
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testGroupByKeyEmpty() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output).empty();

    p.run();
  }

  @Test
  public void testGroupByKeyNonDeterministic() throws Exception {

    List<KV<Map<String, String>, Integer>> ungroupedPairs = Arrays.asList();

    Pipeline p = TestPipeline.create();

    PCollection<KV<Map<String, String>, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(
                KvCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
                    BigEndianIntegerCoder.of())));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must be deterministic");
    input.apply(GroupByKey.<Map<String, String>, Integer>create());
  }

  @Test
  public void testIdentityWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    p.run();

    Assert.assertTrue(output.getWindowingStrategy().getWindowFn().isCompatible(
        FixedWindows.of(Duration.standardMinutes(1))));
  }

  @Test
  public void testWindowFnInvalidation() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    p.run();

    Assert.assertTrue(
        output.getWindowingStrategy().getWindowFn().isCompatible(
            new InvalidWindows(
                "Invalid",
                Sessions.withGapDuration(
                    Duration.standardMinutes(1)))));
  }

  /**
   * Create a test pipeline that uses the {@link DataflowPipelineRunner} so that {@link GroupByKey}
   * is not expanded. This is used for verifying that even without expansion the proper errors show
   * up.
   */
  private Pipeline createTestServiceRunner() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("someproject");
    options.setStagingLocation("gs://staging");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setDataflowClient(null);
    return Pipeline.create(options);
  }

  private Pipeline createTestDirectRunner() {
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    options.setRunner(DirectPipelineRunner.class);
    return Pipeline.create(options);
  }

  @Test
  public void testInvalidWindowsDirect() {
    Pipeline p = createTestDirectRunner();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("GroupByKey must have a valid Window merge function");
    input
        .apply("GroupByKey", GroupByKey.<String, Integer>create())
        .apply("GroupByKeyAgain", GroupByKey.<String, Iterable<Integer>>create());
  }

  @Test
  public void testInvalidWindowsService() {
    Pipeline p = createTestServiceRunner();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("GroupByKey must have a valid Window merge function");
    input
        .apply("GroupByKey", GroupByKey.<String, Integer>create())
        .apply("GroupByKeyAgain", GroupByKey.<String, Iterable<Integer>>create());
  }

  @Test
  public void testRemerge() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Iterable<Integer>>>> middle = input
        .apply("GroupByKey", GroupByKey.<String, Integer>create())
        .apply("Remerge", Window.<KV<String, Iterable<Integer>>>remerge())
        .apply("GroupByKeyAgain", GroupByKey.<String, Iterable<Integer>>create())
        .apply("RemergeAgain", Window.<KV<String, Iterable<Iterable<Integer>>>>remerge());

    p.run();

    Assert.assertTrue(
        middle.getWindowingStrategy().getWindowFn().isCompatible(
            Sessions.withGapDuration(Duration.standardMinutes(1))));
  }

  @Test
  public void testGroupByKeyDirectUnbounded() {
    Pipeline p = createTestDirectRunner();

    PCollection<KV<String, Integer>> input =
        p.apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> apply(PBegin input) {
                return PCollection.<KV<String, Integer>>createPrimitiveOutputInternal(
                        input.getPipeline(),
                        WindowingStrategy.globalDefault(),
                        PCollection.IsBounded.UNBOUNDED)
                    .setTypeDescriptorInternal(new TypeDescriptor<KV<String, Integer>>() {});
              }
            });

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "GroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow without "
            + "a trigger. Use a Window.into or Window.triggering transform prior to GroupByKey.");

    input.apply("GroupByKey", GroupByKey.<String, Integer>create());
  }

  @Test
  public void testGroupByKeyServiceUnbounded() {
    Pipeline p = createTestServiceRunner();

    PCollection<KV<String, Integer>> input =
        p.apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> apply(PBegin input) {
                return PCollection.<KV<String, Integer>>createPrimitiveOutputInternal(
                        input.getPipeline(),
                        WindowingStrategy.globalDefault(),
                        PCollection.IsBounded.UNBOUNDED)
                    .setTypeDescriptorInternal(new TypeDescriptor<KV<String, Integer>>() {});
              }
            });

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "GroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow without "
        + "a trigger. Use a Window.into or Window.triggering transform prior to GroupByKey.");

    input.apply("GroupByKey", GroupByKey.<String, Integer>create());
  }

  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function customized to actually be the same as the default, the earlier of
   * the two values.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testOutputTimeFnEarliest() {
    Pipeline pipeline = TestPipeline.create();

    pipeline.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp()))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new AssertTimestamp(new Instant(0))));

    pipeline.run();
  }


  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function customized to use the latest value.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testOutputTimeFnLatest() {
    Pipeline pipeline = TestPipeline.create();

    pipeline.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withOutputTimeFn(OutputTimeFns.outputAtLatestInputTimestamp()))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new AssertTimestamp(new Instant(10))));

    pipeline.run();
  }

  private static class AssertTimestamp<K, V> extends DoFn<KV<K, V>, Void> {
    private final Instant timestamp;

    public AssertTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      assertThat(c.timestamp(), equalTo(timestamp));
    }
  }

  @Test
  public void testGroupByKeyGetName() {
    Assert.assertEquals("GroupByKey", GroupByKey.<String, Integer>create().getName());
  }
}
