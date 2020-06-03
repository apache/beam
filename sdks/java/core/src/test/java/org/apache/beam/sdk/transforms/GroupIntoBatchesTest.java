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
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test Class for {@link GroupIntoBatches}. */
@RunWith(JUnit4.class)
public class GroupIntoBatchesTest implements Serializable {
  private static final int BATCH_SIZE = 5;
  private static final long EVEN_NUM_ELEMENTS = 10;
  private static final long ODD_NUM_ELEMENTS = 11;
  private static final int ALLOWED_LATENESS = 0;
  private static final Logger LOG = LoggerFactory.getLogger(GroupIntoBatchesTest.class);
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
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
  @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesStatefulParDo.class})
  public void testInGlobalWindow() {
    PCollection<KV<String, Iterable<String>>> collection =
        pipeline
            .apply("Input data", Create.of(data))
            .apply(GroupIntoBatches.ofSize(BATCH_SIZE))
            // set output coder
            .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())));
    PAssert.that("Incorrect batch size in one or more elements", collection)
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Iterable<String>>>, Void>() {

              private boolean checkBatchSizes(Iterable<KV<String, Iterable<String>>> listToCheck) {
                for (KV<String, Iterable<String>> element : listToCheck) {
                  if (Iterables.size(element.getValue()) != BATCH_SIZE) {
                    return false;
                  }
                }
                return true;
              }

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

  /** test behavior when the number of input elements is not evenly divisible by batch size. */
  @Test
  @Category({NeedsRunner.class, UsesTimersInParDo.class, UsesStatefulParDo.class})
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

              private boolean checkBatchSizes(Iterable<KV<String, Iterable<String>>> listToCheck) {
                for (KV<String, Iterable<String>> element : listToCheck) {
                  // number of elements should be less than or equal to BATCH_SIZE
                  if (Iterables.size(element.getValue()) > BATCH_SIZE) {
                    return false;
                  }
                }
                return true;
              }

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
    NeedsRunner.class,
    UsesTimersInParDo.class,
    UsesTestStream.class,
    UsesStatefulParDo.class
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
                    "*** ELEMENT: ({},{}) *** with timestamp %s in window %s",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp().toString(),
                    window.toString());
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
}
