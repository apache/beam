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
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.Event;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.testing.UsesOnWindowExpiration;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTestStreamWithProcessingTime;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test Class for {@link GroupIntoBatches}. */
@RunWith(JUnit4.class)
public class GroupIntoBatchesTest implements Serializable {

  private static final int BATCH_SIZE = 5;
  private static final long BATCH_SIZE_BYTES = 25;
  private static final long EVEN_NUM_ELEMENTS = 10;
  private static final long ODD_NUM_ELEMENTS = 11;
  private static final int ALLOWED_LATENESS = 0;
  private static final Logger LOG = LoggerFactory.getLogger(GroupIntoBatchesTest.class);
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);
  private transient ArrayList<KV<String, String>> data = createTestData(EVEN_NUM_ELEMENTS);

  private static ArrayList<KV<String, String>> createTestData(long numElements) {
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      int index = i % scientists.length;
      KV<String, String> element = KV.of("key", scientists[index]);
      data.add(element);
    }
    return data;
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testInGlobalWindowBatchSizeCount() {
    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", Create.of(data))
            .apply(GroupIntoBatches.ofSize(BATCH_SIZE))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Iterable<String>>> input) {
                assertTrue(checkBatchSizes(input));
                return null;
              }
            });
    PAssert.thatSingleton("Incorrect collection size", collection.apply("Count", Count.globally()))
        .isEqualTo(EVEN_NUM_ELEMENTS / BATCH_SIZE);
    pipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testInGlobalWindowBatchSizeByteSize() {
    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", Create.of(data))
            .apply(GroupIntoBatches.ofByteSize(BATCH_SIZE_BYTES))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Iterable<String>>> input) {
                assertTrue(checkBatchByteSizes(input));
                return null;
              }
            });
    PAssert.thatSingleton("Incorrect collection size", collection.apply("Count", Count.globally()))
        .isEqualTo(4L);
    pipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTestStream.class,
    UsesTimersInParDo.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testInGlobalWindowBatchSizeByteSizeFn() {
    SerializableFunction<String, Long> getElementByteSizeFn =
        s -> {
          try {
            return 2 * StringUtf8Coder.of().getEncodedElementByteSize(s);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    // to ensure ordered processing
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(Instant.EPOCH);

    long offset = 0L;
    for (KV<String, String> kv : data) {
      streamBuilder =
          streamBuilder.addElements(
              TimestampedValue.of(kv, Instant.EPOCH.plus(Duration.standardSeconds(offset))));
      offset++;
    }

    // fire them all at once
    TestStream<KV<String, String>> stream = streamBuilder.advanceWatermarkToInfinity();

    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", stream)
            .apply(GroupIntoBatches.ofByteSize(BATCH_SIZE_BYTES, getElementByteSizeFn))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Iterable<String>>> input) {
                assertTrue(checkBatchByteSizes(input, getElementByteSizeFn));
                assertEquals("Invalid batch count", 9L, Iterables.size(input));
                return null;
              }
            });
    pipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testWithShardedKeyInGlobalWindow() {
    // Since with default sharding, the number of subshards of a key is nondeterministic, create
    // a large number of input elements and a small batch size and check there is no batch larger
    // than the specified size.
    int numElements = 10000;
    int batchSize = 5;
    PCollection<KV<ShardedKey<String>, Iterable<String>>> collection =
        pipeline
            .apply("Input data", Create.of(createTestData(numElements)))
            .apply(GroupIntoBatches.<String, String>ofSize(batchSize).withShardedKey())
            .setCoder(
                KvCoder.of(
                    ShardedKey.Coder.of(StringUtf8Coder.of()),
                    IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<ShardedKey<String>, Iterable<String>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<ShardedKey<String>, Iterable<String>>> input) {
                assertTrue(checkBatchSizes(input, batchSize));
                return null;
              }
            });
    PCollection<KV<Integer, Long>> numBatchesbyBatchSize =
        collection
            .apply(
                "KeyByBatchSize",
                MapElements.via(
                    new SimpleFunction<
                        KV<ShardedKey<String>, Iterable<String>>, KV<Integer, Integer>>() {
                      @Override
                      public KV<Integer, Integer> apply(
                          KV<ShardedKey<String>, Iterable<String>> input) {
                        int batchSize = 0;
                        for (String ignored : input.getValue()) {
                          batchSize++;
                        }
                        return KV.of(batchSize, 1);
                      }
                    }))
            .apply("CountBatchesBySize", Count.perKey());
    PAssert.that("Expecting majority of the batches are full", numBatchesbyBatchSize)
        .satisfies(
            (SerializableFunction<Iterable<KV<Integer, Long>>, Void>)
                listOfBatchSize -> {
                  Long numFullBatches = 0L;
                  Long totalNumBatches = 0L;
                  for (KV<Integer, Long> batchSizeAndCount : listOfBatchSize) {
                    if (batchSizeAndCount.getKey() == batchSize) {
                      numFullBatches += batchSizeAndCount.getValue();
                    }
                    totalNumBatches += batchSizeAndCount.getValue();
                  }
                  assertTrue(
                      String.format(
                          "total number of batches should be in the range [%d, %d] but got %d",
                          numElements, numElements / batchSize, numFullBatches),
                      numFullBatches <= numElements && numFullBatches >= numElements / batchSize);
                  assertTrue(
                      String.format(
                          "number of full batches vs. total number of batches in total: %d vs. %d",
                          numFullBatches, totalNumBatches),
                      numFullBatches > totalNumBatches / 2);
                  return null;
                });
    if (pipeline.getOptions().getRunner().getSimpleName().equals("DirectRunner")) {
      pipeline.runWithAdditionalOptionArgs(ImmutableList.of("--targetParallelism=1"));
    } else {
      pipeline.run();
    }
  }

  /** test behavior when the number of input elements is not evenly divisible by batch size. */
  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testWithUnevenBatches() {
    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", Create.of(createTestData(ODD_NUM_ELEMENTS)))
            .apply(GroupIntoBatches.ofSize(BATCH_SIZE))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Iterable<String>>> input) {
                assertTrue(checkBatchSizes(input));
                return null;
              }
            });
    PAssert.thatSingleton("Incorrect collection size", collection.apply("Count", Count.globally()))
        .isEqualTo(
            // round up division for positive numbers
            // https://math.stackexchange.com/questions/2591316/proof-for-integer-division-algorithm-that-rounds-up.
            (ODD_NUM_ELEMENTS + BATCH_SIZE - 1) / BATCH_SIZE);
    pipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesTestStream.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testInStreamingMode() {
    int timestampInterval = 1;
    Instant startInstant = new Instant(0L);
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(startInstant);
    long offset = 0L;
    for (KV<String, String> element : data) {
      streamBuilder =
          streamBuilder.addElements(
              TimestampedValue.of(
                  element,
                  startInstant.plus(Duration.standardSeconds(offset * timestampInterval))));
      offset++;
    }
    final long windowDuration = 6;
    TestStream<KV<String, String>> stream =
        streamBuilder
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(windowDuration - 1)))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(windowDuration + 1)))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(EVEN_NUM_ELEMENTS)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> inputCollection =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, String>>into(
                        FixedWindows.of(Duration.standardSeconds(windowDuration)))
                    .withAllowedLateness(Duration.millis(ALLOWED_LATENESS)));
    inputCollection.apply(
        ParDo.of(
            new DoFn<KV<String, String>, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c, BoundedWindow window) {
                LOG.debug(
                    "*** ELEMENT: ({},{}) *** with timestamp {} in window {}",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp(),
                    window);
              }
            }));

    PCollection<KV<String, Iterable<String>>> outputCollection =
        inputCollection
            .apply(GroupIntoBatches.ofSize(BATCH_SIZE))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    // elements have the same key and collection is divided into windows,
    // so Count.perKey values are the number of elements in windows
    PCollection<KV<String, Long>> countOutput =
        outputCollection.apply(
            "Count elements in windows after applying GroupIntoBatches", Count.perKey());

    PAssert.that("Wrong number of elements in windows after GroupIntoBatches", countOutput)
        .satisfies(
            input -> {
              Iterator<KV<String, Long>> inputIterator = input.iterator();
              // first element
              long count0 = inputIterator.next().getValue();
              // window duration is 6 and batch size is 5, so there should be 2 elements in the
              // window (flush because batchSize reached and for end of window reached)
              assertEquals("Wrong number of elements in first window", 2, count0);
              // second element
              long count1 = inputIterator.next().getValue();
              // collection is 10 elements, there is only 4 elements left, so there should be only
              // one element in the window (flush because end of window/collection reached)
              assertEquals("Wrong number of elements in second window", 1, count1);
              // third element
              return null;
            });

    PAssert.that("Incorrect output collection after GroupIntoBatches", outputCollection)
        .satisfies(
            input -> {
              Iterator<KV<String, Iterable<String>>> inputIterator = input.iterator();
              // first element
              int size0 = Iterables.size(inputIterator.next().getValue());
              // window duration is 6 and batch size is 5, so output batch size should de 5
              // (flush because of batchSize reached)
              assertEquals("Wrong first element batch Size", 5, size0);
              // second element
              int size1 = Iterables.size(inputIterator.next().getValue());
              // there is only one element left in the window so batch size should be 1
              // (flush because of end of window reached)
              assertEquals("Wrong second element batch Size", 1, size1);
              // third element
              int size2 = Iterables.size(inputIterator.next().getValue());
              // collection is 10 elements, there is only 4 left, so batch size should be 4
              // (flush because end of collection reached)
              assertEquals("Wrong third element batch Size", 4, size2);
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesTestStream.class,
    UsesTestStreamWithProcessingTime.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testBufferingTimerInFixedWindow() {
    final Duration windowDuration = Duration.standardSeconds(4);
    final Duration maxBufferingDuration = Duration.standardSeconds(5);

    Instant startInstant = new Instant(0L);
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(startInstant);
    long offset = 0L;
    int timestampInterval = 1;
    for (KV<String, String> element : data) {
      streamBuilder =
          streamBuilder
              .addElements(
                  TimestampedValue.of(
                      element,
                      startInstant.plus(Duration.standardSeconds(offset * timestampInterval))))
              // Advance the processing time by 2 secs every time an element is processed. The max
              // buffering duration allowed is 5 secs, so the timer should be fired after receiving
              // three elements, i.e., the batch size will not exceed 3.
              .advanceProcessingTime(Duration.standardSeconds(2));
      offset++;
    }

    TestStream<KV<String, String>> stream =
        streamBuilder
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(EVEN_NUM_ELEMENTS)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> inputCollection =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, String>>into(FixedWindows.of(windowDuration))
                    .withAllowedLateness(Duration.millis(ALLOWED_LATENESS)));
    inputCollection.apply(
        ParDo.of(
            new DoFn<KV<String, String>, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c, BoundedWindow window) {
                LOG.debug(
                    "*** ELEMENT: ({},{}) *** with timestamp {} in window {}",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp(),
                    window);
              }
            }));

    PCollection<KV<String, Iterable<String>>> outputCollection =
        inputCollection
            .apply(
                GroupIntoBatches.<String, String>ofSize(BATCH_SIZE)
                    .withMaxBufferingDuration(maxBufferingDuration))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    // Elements have the same key and collection is divided into windows,
    // so Count.perKey values are the number of elements in windows
    PCollection<KV<String, Long>> countOutput =
        outputCollection.apply(
            "Count elements in windows after applying GroupIntoBatches", Count.perKey());

    PAssert.that("Wrong number of elements in windows after GroupIntoBatches", countOutput)
        .satisfies(
            input -> {
              Iterator<KV<String, Long>> inputIterator = input.iterator();
              // first element
              long count0 = inputIterator.next().getValue();
              // window duration is 4 , so there should be 2 elements in the window (flush because
              // maxBufferingDuration reached and the end of window reached)
              assertEquals("Wrong number of elements in first window", 2, count0);
              // second element
              long count1 = inputIterator.next().getValue();
              // same as the first window
              assertEquals("Wrong number of elements in second window", 2, count1);
              long count2 = inputIterator.next().getValue();
              // collection has 10 elements, there is only 2 elements left, so there should be only
              // one element in the window (flush because end of window reached)
              assertEquals("Wrong number of elements in third window", 1, count2);
              return null;
            });

    PAssert.that("Incorrect output collection after GroupIntoBatches", outputCollection)
        .satisfies(
            input -> {
              Iterator<KV<String, Iterable<String>>> inputIterator = input.iterator();
              // first element
              int size0 = Iterables.size(inputIterator.next().getValue());
              // max buffering duration is 2 and the buffering deadline is set when processing the
              // first element in this window, so output batch size should de 3
              // (flush because of maxBufferingDuration reached)
              assertEquals("Wrong first element batch Size", 3, size0);
              // second element
              int size1 = Iterables.size(inputIterator.next().getValue());
              // there is only one element left in the first window so batch size should be 1
              // (flush because of end of window reached)
              assertEquals("Wrong second element batch Size", 1, size1);
              // third element
              int size2 = Iterables.size(inputIterator.next().getValue());
              // same as the first window
              assertEquals("Wrong third element batch Size", 3, size2);
              // forth element
              int size3 = Iterables.size(inputIterator.next().getValue());
              // same as the first window
              assertEquals("Wrong third element batch Size", 1, size3);
              // fifth element
              int size4 = Iterables.size(inputIterator.next().getValue());
              // collection is 10 elements, there is only 2 left, so batch size should be 2
              // (flush because end of window reached)
              assertEquals("Wrong forth element batch Size", 2, size4);
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesTestStream.class,
    UsesTestStreamWithProcessingTime.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testBufferingTimerInGlobalWindow() {
    final Duration maxBufferingDuration = Duration.standardSeconds(5);

    Instant startInstant = new Instant(0L);
    long offset = 0L;
    int timestampInterval = 1;
    List<Event<KV<String, String>>> events = new ArrayList<>();
    List<TimestampedValue<KV<String, String>>> elements1 = new ArrayList<>();
    for (KV<String, String> element : createTestData(EVEN_NUM_ELEMENTS / 2)) {
      elements1.add(
          TimestampedValue.of(
              element, startInstant.plus(Duration.standardSeconds(offset * timestampInterval))));
      offset++;
    }
    events.add(ElementEvent.add(elements1));
    events.add(ProcessingTimeEvent.advanceBy(Duration.standardSeconds(100)));

    List<TimestampedValue<KV<String, String>>> elements2 = new ArrayList<>();
    for (KV<String, String> element : createTestData(EVEN_NUM_ELEMENTS / 2)) {
      elements2.add(
          TimestampedValue.of(
              element, startInstant.plus(Duration.standardSeconds(offset * timestampInterval))));
      offset++;
    }
    events.add(ElementEvent.add(elements2));
    events.add(ProcessingTimeEvent.advanceBy(Duration.standardSeconds(100)));
    events.add(
        WatermarkEvent.advanceTo(startInstant.plus(Duration.standardSeconds(EVEN_NUM_ELEMENTS))));
    TestStream<KV<String, String>> stream =
        TestStream.fromRawEvents(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), events);

    PCollection<KV<String, String>> inputCollection =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, String>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .discardingFiredPanes());

    inputCollection.apply(
        ParDo.of(
            new DoFn<KV<String, String>, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c, BoundedWindow window) {
                LOG.debug(
                    "*** ELEMENT: ({},{}) *** with timestamp {} in window {}",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp(),
                    window);
              }
            }));

    // Set a batch size larger than the total number of elements. Since we're in a global window, we
    // would have been waiting for all the elements without the buffering time limit.
    PCollection<KV<String, Iterable<String>>> outputCollection =
        inputCollection
            .apply(
                GroupIntoBatches.<String, String>ofSize(EVEN_NUM_ELEMENTS + 5)
                    .withMaxBufferingDuration(maxBufferingDuration))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));

    // Elements have the same key and collection is divided into windows,
    // so Count.perKey values are the number of elements in windows
    PCollection<KV<String, Long>> countOutput =
        outputCollection.apply(
            "Count elements in windows after applying GroupIntoBatches", Count.perKey());

    PAssert.that("Wrong number of elements in windows after GroupIntoBatches", countOutput)
        .satisfies(
            input -> {
              Iterator<KV<String, Long>> inputIterator = input.iterator();
              long count = inputIterator.next().getValue();
              assertEquals("Wrong number of elements in global window", 2, count);
              return null;
            });

    PAssert.that("Incorrect output collection after GroupIntoBatches", outputCollection)
        .satisfies(
            input -> {
              Iterator<KV<String, Iterable<String>>> inputIterator = input.iterator();
              int size1 = Iterables.size(inputIterator.next().getValue());
              assertEquals("Wrong first element batch Size", EVEN_NUM_ELEMENTS / 2, size1);
              int size2 = Iterables.size(inputIterator.next().getValue());
              assertEquals("Wrong second element batch Size", EVEN_NUM_ELEMENTS / 2, size2);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @SuppressWarnings("InlineMeInliner") // inline `Strings.repeat()` - Java 11+ API only
  @Test
  @Category({
    ValidatesRunner.class,
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesTestStream.class,
    UsesStatefulParDo.class,
    UsesOnWindowExpiration.class
  })
  public void testMultipleLimitsAtOnceInGlobalWindowBatchSizeCountAndBatchSizeByteSize() {
    // with using only one of the limits the result would be only 2 batches,
    // if we have 3 both limits are exercised
    List<KV<String, String>> dataToUse =
        Lists.newArrayList(
                "a-1",
                "a-2",
                // batch byte size limit would be reached with the next one so "firing" current
                // batch content (BATCH_SIZE_BYTES = 25)
                "b-3" + Strings.repeat("-", 100),
                // batch byte size is over the limit, but we have a single element that we can't
                // split to smaller batches (BATCH_SIZE_BYTES = 25)
                "c-4",
                "c-5",
                "c-6",
                "c-7",
                "c-8",
                // count limit reached (BATCH_SIZE = 5)
                "d-9")
            .stream()
            .map(s -> KV.of("key", s))
            .collect(Collectors.toList());

    // to ensure ordered processing
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(Instant.EPOCH);

    long offset = 0L;
    for (KV<String, String> kv : dataToUse) {
      streamBuilder =
          streamBuilder.addElements(
              TimestampedValue.of(kv, Instant.EPOCH.plus(Duration.standardSeconds(offset))));
      offset++;
    }

    // fire them all at once
    TestStream<KV<String, String>> stream = streamBuilder.advanceWatermarkToInfinity();

    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", stream)
            .apply(
                GroupIntoBatches.<String, String>ofSize(BATCH_SIZE).withByteSize(BATCH_SIZE_BYTES))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {

              private void assertExpectedBatchPrefix(
                  Iterable<KV<String, Iterable<String>>> listToCheck) {
                for (KV<String, Iterable<String>> element : listToCheck) {
                  Set<String> batchPrefixes =
                      Streams.stream(element.getValue())
                          .map(s -> Iterables.get(Splitter.on('-').split(s), 0))
                          .collect(Collectors.toSet());
                  assertEquals("Found invalid batching: " + listToCheck, 1, batchPrefixes.size());
                }
              }

              @Override
              public Void apply(Iterable<KV<String, Iterable<String>>> input) {
                assertTrue(checkBatchSizes(input));
                assertTrue(checkBatchByteSizes(input));
                assertExpectedBatchPrefix(input);
                assertEquals(
                    Lists.newArrayList(2, 1, 5, 1),
                    Streams.stream(input)
                        .map(KV::getValue)
                        .map(Iterables::size)
                        .collect(Collectors.toList()));
                return null;
              }
            });

    pipeline.run();
  }

  private static <K> boolean checkBatchSizes(Iterable<KV<K, Iterable<String>>> listToCheck) {
    return checkBatchSizes(listToCheck, BATCH_SIZE);
  }

  private static <K> boolean checkBatchSizes(
      Iterable<KV<K, Iterable<String>>> listToCheck, int batchSize) {
    for (KV<?, Iterable<String>> element : listToCheck) {
      // number of elements should be less than or equal to the batch size
      if (Iterables.size(element.getValue()) > batchSize) {
        return false;
      }
    }
    return true;
  }

  private static <K> boolean checkBatchByteSizes(Iterable<KV<K, Iterable<String>>> listToCheck) {
    return checkBatchByteSizes(
        listToCheck,
        s -> {
          try {
            return StringUtf8Coder.of().getEncodedElementByteSize(s);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static <K> boolean checkBatchByteSizes(
      Iterable<KV<K, Iterable<String>>> listToCheck,
      SerializableFunction<String, Long> getElementByteSizeFn) {
    for (KV<?, Iterable<String>> element : listToCheck) {
      List<String> batchElements = Lists.newArrayList(element.getValue());
      if (batchElements.size() == 1) {
        // if we have a single element that is over the batch size byte limit, we can't do anything
        // than to fire it as a 1-element batch
        continue;
      }
      long byteSize = 0;
      for (String str : batchElements) {
        try {
          byteSize += getElementByteSizeFn.apply(str);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        // size of elements should be less than or equal to the batch size
        if (byteSize > BATCH_SIZE_BYTES) {
          return false;
        }
      }
    }
    return true;
  }
}
