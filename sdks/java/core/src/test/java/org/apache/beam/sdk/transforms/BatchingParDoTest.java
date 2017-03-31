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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
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
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test Class for {@link BatchingParDo}. */
@RunWith(JUnit4.class)
public class BatchingParDoTest implements Serializable {
  private static final int BATCH_SIZE = 3;
  private static final long NUM_ELEMENTS = 100;
  private static final int ALLOWED_LATENESS = 0;
  private static final int TIMESTAMP_INTERVAL = 1;
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchingParDoTest.class);
  private transient ArrayList<KV<String, String>> data = createTestData();
  private static SimpleFunction<Iterable<String>, Iterable<String>> perBatchFn;
  private static Instant startInstant;
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void initialize() {
    startInstant = new Instant(0);
    perBatchFn =
        new SimpleFunction<Iterable<String>, Iterable<String>>() {
          @Override
          public Iterable<String> apply(Iterable<String> input) {
            ArrayList<String> output = new ArrayList<>();
            for (String element : input) {
              output.add(element + "2");
            }
            return output;
          }
        };
  }

  private static ArrayList<KV<String, String>> createTestData() {
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
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      int index = i % scientists.length;
      KV<String, String> element = KV.of("key", scientists[index]);
      data.add(element);
    }
    return data;
  }

  @Ignore("timer is not supported by DoFnTester")
  @Test
  public void testUnderlyingDoFn() throws Exception {
    DoFnTester<KV<String, String>, KV<String, String>> fnTester =
        DoFnTester.of(
            new BatchingParDo.BatchingDoFn<>(
                BATCH_SIZE,
                perBatchFn,
                new Duration(ALLOWED_LATENESS),
                StringUtf8Coder.of(),
                StringUtf8Coder.of()));
    int nbElementsProcessed = 0;
    for (KV<String, String> element : data) {
      fnTester.processElement(element);
      nbElementsProcessed++;
      List<KV<String, String>> output = fnTester.takeOutputElements();
      // end of batch
      if ((nbElementsProcessed % BATCH_SIZE) == 0) {
        assertEquals(
            String.format("since last batch we should have processed %s elements", BATCH_SIZE),
            BATCH_SIZE,
            output.size());
        assertTrue(
            "All elements since last batch should have been processed",
            checkAllElementsProcessing(output, Processing.PROCESSED));
      } else {
        // not end of batch
        assertTrue(
            "we should have processed no elements since last batch",
            checkAllElementsProcessing(output, Processing.UNPROCESSED));
      }
    }
  }

  @Test
  @Category({RunnableOnService.class, UsesTimersInParDo.class, UsesStatefulParDo.class})
  public void testInGlobalWindow() {
    PCollection<KV<String, String>> collection =
        pipeline
            .apply("Input data", Create.of(data))
            .apply(BatchingParDo.<String, String, String>via(BATCH_SIZE, perBatchFn))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    PAssert.that(collection).satisfies(new CheckAllElementsProcessingFn());
    PAssert.thatSingleton(collection.apply("Count", Count.<KV<String, String>>globally()))
        .isEqualTo(NUM_ELEMENTS);
    pipeline.run();
  }

  @Test
  @Category({RunnableOnService.class, UsesTimersInParDo.class, UsesTestStream.class, UsesStatefulParDo.class})
  public void testInStreamingMode() {
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(startInstant);
    long offset = 0L;
    for (KV<String, String> element : data) {
      streamBuilder = streamBuilder.addElements(TimestampedValue.of(element, startInstant.plus(Duration.standardSeconds(offset * TIMESTAMP_INTERVAL))));
      offset ++;
    }
    final long windowDuration = 5;
    TestStream<KV<String, String>> stream =
        streamBuilder
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(windowDuration - 1)))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(windowDuration + 1)))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(NUM_ELEMENTS)))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, String>> inputCollection =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, String>>into(
                    FixedWindows.of(Duration.standardSeconds(windowDuration))));
    inputCollection.apply(
        ParDo.of(
            new DoFn<KV<String, String>, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c, BoundedWindow window) {
                LOGGER.debug(
                    "*** ELEMENT: (%s,%s) *** with timestamp %s in window %s",
                    c.element().getKey(),
                    c.element().getValue(),
                    c.timestamp().toString(),
                    window.toString());
              }
            }));

    // elements have the same key and collection is divided into windows,
    // so Count.perKey values are the number of elements in windows
    PCollection<KV<String, Long>> countInput =
        inputCollection.apply(
            "Count elements in windows before applying batchingParDo",
            Count.<String, String>perKey());
    PAssert.that("Wrong number of elements in windows before BatchingParDo", countInput)
        .satisfies(new CheckValuesFn(windowDuration));

    PCollection<KV<String, String>> outputCollection =
        inputCollection
            .apply(BatchingParDo.<String, String, String>via(BATCH_SIZE, perBatchFn))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that("All elements have not been processed", outputCollection)
        .satisfies(new CheckAllElementsProcessingFn());

    // elements have the same key and collection is divided into windows,
    // so Count.perKey values are the number of elements in windows
    PCollection<KV<String, Long>> countOutput =
        outputCollection.apply(
            "Count elements in windows after applying batchingParDo",
            Count.<String, String>perKey());

    PAssert.that("Wrong number of elements in windows after BatchingParDo", countOutput)
        .satisfies(new CheckValuesFn(windowDuration));
    pipeline.run().waitUntilFinish();
  }

  private boolean checkAllElementsProcessing(
      Iterable<KV<String, String>> listToCheck, Processing processing) {
    for (KV<String, String> element : listToCheck) {
      if (processing == Processing.PROCESSED) {
        if (!element.getValue().matches(".*2")) {
          return false;
        }
      } else {
        if (element.getValue().matches(".*2")) {
          return false;
        }
      }
    }
    return true;
  }

  private enum Processing {
    PROCESSED,
    UNPROCESSED
  }

  private class CheckAllElementsProcessingFn
      implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    @Override
    public Void apply(Iterable<KV<String, String>> input) {
      assertTrue(
          "all elements of the collection have not been processed ",
          checkAllElementsProcessing(input, Processing.PROCESSED));
      return null;
    }
  }

  private class CheckValuesFn implements SerializableFunction<Iterable<KV<String, Long>>, Void> {
    private long num;

    private CheckValuesFn(long num) {
      this.num = num;
    }

    @Override
    public Void apply(Iterable<KV<String, Long>> input) {
      for (KV<String, Long> element : input) {
        assertThat(element.getValue(), Matchers.equalTo(num));
      }
      return null;
    }
  }
}
