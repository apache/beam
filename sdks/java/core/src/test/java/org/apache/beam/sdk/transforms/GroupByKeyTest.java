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

import static org.apache.beam.sdk.TestUtils.KvMatcher.isKv;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for GroupByKey.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByKeyTest {

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(ValidatesRunner.class)
  public void testGroupByKey() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),
        KV.of("k5", Integer.MAX_VALUE),
        KV.of("k5", Integer.MIN_VALUE),
        KV.of("k2", 66),
        KV.of("k1", 4),
        KV.of("k2", -33),
        KV.of("k3", 0));

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    PAssert.that(output)
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
  @Category(ValidatesRunner.class)
  public void testGroupByKeyAndWindows() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),  // window [0, 5)
        KV.of("k5", Integer.MAX_VALUE), // window [0, 5)
        KV.of("k5", Integer.MIN_VALUE), // window [0, 5)
        KV.of("k2", 66), // window [0, 5)
        KV.of("k1", 4),  // window [5, 10)
        KV.of("k2", -33),  // window [5, 10)
        KV.of("k3", 0));  // window [5, 10)

    PCollection<KV<String, Integer>> input =
        p.apply(Create.timestamped(ungroupedPairs, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(Window.<KV<String, Integer>>into(FixedWindows.of(new Duration(5))))
             .apply(GroupByKey.<String, Integer>create());

    PAssert.that(output)
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
  @Category(ValidatesRunner.class)
  public void testGroupByKeyEmpty() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    PAssert.that(output).empty();

    p.run();
  }

  @Test
  public void testGroupByKeyNonDeterministic() throws Exception {

    List<KV<Map<String, String>, Integer>> ungroupedPairs = Arrays.asList();

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
  @Category(NeedsRunner.class)
  public void testIdentityWindowFnPropagation() {

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
  @Category(NeedsRunner.class)
  public void testWindowFnInvalidation() {

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

  @Test
  public void testInvalidWindowsDirect() {

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
  @Category(NeedsRunner.class)
  public void testRemerge() {

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

    PCollection<KV<String, Integer>> input =
        p.apply(
            new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
              @Override
              public PCollection<KV<String, Integer>> expand(PBegin input) {
                return PCollection.<KV<String, Integer>>createPrimitiveOutputInternal(
                        input.getPipeline(),
                        WindowingStrategy.globalDefault(),
                        PCollection.IsBounded.UNBOUNDED)
                    .setTypeDescriptor(new TypeDescriptor<KV<String, Integer>>() {});
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
  @Category(ValidatesRunner.class)
  public void testTimestampCombinerEarliest() {

    p.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new AssertTimestamp(new Instant(0))));

    p.run();
  }


  /**
   * Tests that when two elements are combined via a GroupByKey their output timestamp agrees
   * with the windowing function customized to use the latest value.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testTimestampCombinerLatest() {
    p.apply(
        Create.timestamped(
            TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
            TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
        .apply(Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
            .withTimestampCombiner(TimestampCombiner.LATEST))
        .apply(GroupByKey.<Integer, String>create())
        .apply(ParDo.of(new AssertTimestamp(new Instant(10))));

    p.run();
  }

  private static class AssertTimestamp<K, V> extends DoFn<KV<K, V>, Void> {
    private final Instant timestamp;

    public AssertTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      assertThat(c.timestamp(), equalTo(timestamp));
    }
  }

  @Test
  public void testGroupByKeyGetName() {
    Assert.assertEquals("GroupByKey", GroupByKey.<String, Integer>create().getName());
  }

  @Test
  public void testDisplayData() {
    GroupByKey<String, String> groupByKey = GroupByKey.create();
    GroupByKey<String, String> groupByFewKeys = GroupByKey.createWithFewKeys();

    DisplayData gbkDisplayData = DisplayData.from(groupByKey);
    DisplayData fewKeysDisplayData = DisplayData.from(groupByFewKeys);

    assertThat(gbkDisplayData.items(), empty());
    assertThat(fewKeysDisplayData, hasDisplayItem("fewKeys", true));
  }


  /**
   * Verify that runners correctly hash/group on the encoded value
   * and not the value itself.
   */
  @Test
  @Category(ValidatesRunner.class)
  public void testGroupByKeyWithBadEqualsHashCode() throws Exception {
    final int numValues = 10;
    final int numKeys = 5;

    p.getCoderRegistry().registerCoderProvider(
        CoderProviders.fromStaticMethods(BadEqualityKey.class, DeterministicKeyCoder.class));

    // construct input data
    List<KV<BadEqualityKey, Long>> input = new ArrayList<>();
    for (int i = 0; i < numValues; i++) {
      for (int key = 0; key < numKeys; key++) {
        input.add(KV.of(new BadEqualityKey(key), 1L));
      }
    }

    // We first ensure that the values are randomly partitioned in the beginning.
    // Some runners might otherwise keep all values on the machine where
    // they are initially created.
    PCollection<KV<BadEqualityKey, Long>> dataset1 = p
        .apply(Create.of(input))
        .apply(ParDo.of(new AssignRandomKey()))
        .apply(Reshuffle.<Long, KV<BadEqualityKey, Long>>of())
        .apply(Values.<KV<BadEqualityKey, Long>>create());

    // Make the GroupByKey and Count implicit, in real-world code
    // this would be a Count.perKey()
    PCollection<KV<BadEqualityKey, Long>> result = dataset1
        .apply(GroupByKey.<BadEqualityKey, Long>create())
        .apply(Combine.<BadEqualityKey, Long>groupedValues(new CountFn()));

    PAssert.that(result).satisfies(new AssertThatCountPerKeyCorrect(numValues));

    PAssert.that(result.apply(Keys.<BadEqualityKey>create()))
        .satisfies(new AssertThatAllKeysExist(numKeys));

    p.run();
  }

  /**
   * This is a bogus key class that returns random hash values from {@link #hashCode()} and always
   * returns {@code false} for {@link #equals(Object)}. The results of the test are correct if
   * the runner correctly hashes and sorts on the encoded bytes.
   */
  static class BadEqualityKey {
    long key;

    public BadEqualityKey() {}

    public BadEqualityKey(long key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return ThreadLocalRandom.current().nextInt();
    }
  }

  /**
   * Deterministic {@link Coder} for {@link BadEqualityKey}.
   */
  static class DeterministicKeyCoder extends AtomicCoder<BadEqualityKey> {

    public static DeterministicKeyCoder of() {
      return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final DeterministicKeyCoder INSTANCE =
        new DeterministicKeyCoder();

    private DeterministicKeyCoder() {}

    @Override
    public void encode(BadEqualityKey value, OutputStream outStream)
        throws IOException {
      new DataOutputStream(outStream).writeLong(value.key);
    }

    @Override
    public BadEqualityKey decode(InputStream inStream)
        throws IOException {
      return new BadEqualityKey(new DataInputStream(inStream).readLong());
    }

    @Override
    public void verifyDeterministic() {}
  }

  /**
   * Creates a KV that wraps the original KV together with a random key.
   */
  static class AssignRandomKey
      extends DoFn<KV<BadEqualityKey, Long>, KV<Long, KV<BadEqualityKey, Long>>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(ThreadLocalRandom.current().nextLong(), c.element()));
    }
  }

  static class CountFn implements SerializableFunction<Iterable<Long>, Long> {
    @Override
    public Long apply(Iterable<Long> input) {
      long result = 0L;
      for (Long in: input) {
        result += in;
      }
      return result;
    }
  }

  static class AssertThatCountPerKeyCorrect
      implements SerializableFunction<Iterable<KV<BadEqualityKey, Long>>, Void> {
    private final int numValues;

    AssertThatCountPerKeyCorrect(int numValues) {
      this.numValues = numValues;
    }

    @Override
    public Void apply(Iterable<KV<BadEqualityKey, Long>> input) {
      for (KV<BadEqualityKey, Long> val: input) {
        Assert.assertEquals(numValues, (long) val.getValue());
      }
      return null;
    }
  }

  static class AssertThatAllKeysExist
      implements SerializableFunction<Iterable<BadEqualityKey>, Void> {
    private final int numKeys;

    AssertThatAllKeysExist(int numKeys) {
      this.numKeys = numKeys;
    }

    private static <T> Iterable<Object> asStructural(
        final Iterable<T> iterable,
        final Coder<T> coder) {

      return Iterables.transform(
          iterable,
          new Function<T, Object>() {
            @Override
            public Object apply(T input) {
              try {
                return coder.structuralValue(input);
              } catch (Exception e) {
                Assert.fail("Could not structural values.");
                throw new RuntimeException(); // to satisfy the compiler...
              }
            }
          });

    }
    @Override
    public Void apply(Iterable<BadEqualityKey> input) {
      final DeterministicKeyCoder keyCoder = DeterministicKeyCoder.of();

      List<BadEqualityKey> expectedList = new ArrayList<>();
      for (int key = 0; key < numKeys; key++) {
        expectedList.add(new BadEqualityKey(key));
      }

      Iterable<Object> structuralInput = asStructural(input, keyCoder);
      Iterable<Object> structuralExpected = asStructural(expectedList, keyCoder);

      for (Object expected: structuralExpected) {
        assertThat(structuralInput, hasItem(expected));
      }

      return null;
    }
  }
}
