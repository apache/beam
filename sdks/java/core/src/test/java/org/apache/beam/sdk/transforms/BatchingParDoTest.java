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
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchingParDoTest implements Serializable {
  private static final int BATCH_SIZE = 2;
  private static final long NUM_ELEMENTS = 10;
  private static final int ALLOWED_LATENESS = 0;
  private static final int TIMESTAMP_INTERVAL = 1;
  private static final int WINDOW_DURATION = 5;
  private static ArrayList<TimestampedValue<KV<String, String>>> data;
  private static SimpleFunction<Iterable<String>, Iterable<String>> perBatchFn;
  private static BatchingParDo<String, String, String> batchingParDo;
  private static Instant startInstant;
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void initialize() {

    // give time for the pipeline to start so that first element timestamp is in pipeline
    // (relative time to now() because of timer
    startInstant = Instant.now().plus(Duration.standardSeconds(10));
    data = createTestData();
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
    batchingParDo = BatchingParDo.via(BATCH_SIZE, perBatchFn);
  }

  private static ArrayList<TimestampedValue<KV<String, String>>> createTestData() {
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
    ArrayList<TimestampedValue<KV<String, String>>> data = new ArrayList<>();
    long offset = 1;
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      int index = i % scientists.length;
      data.add(
          TimestampedValue.of(
              KV.of("key", scientists[index]), startInstant.plus(Duration.standardSeconds(offset * TIMESTAMP_INTERVAL))));
      offset++;
    }
    return data;
  }

  // timer is not supported by DoFnTester
  @Ignore
  @Test
  public void testUnderlyingDoFn() throws Exception {
    DoFnTester<KV<String, String>, String> fnTester =
        DoFnTester.of(
            new BatchingParDo.BatchingDoFn<String, String, String>(
                BATCH_SIZE, perBatchFn, new Duration(ALLOWED_LATENESS), StringUtf8Coder.of()));
    int nbElementsProcessed = 0;
    for (TimestampedValue<KV<String, String>> element : data) {
      fnTester.processElement(element.getValue());
      nbElementsProcessed++;
      List<String> output = fnTester.takeOutputElements();
      // end of batch
      if ((nbElementsProcessed % BATCH_SIZE) == 0) {
        assertEquals(
            String.format("since last batch we should have processed %s elements", BATCH_SIZE),
            BATCH_SIZE,
            output.size());
        assertTrue(
            "All elements since last batch should have been processed",
            checkAllElementsProcessing(output, Processing.PROCESSED));
      }
      // not end of batch
      else {
        assertTrue(
            "we should have processed no elements since last batch",
            checkAllElementsProcessing(output, Processing.UNPROCESSED));
      }
    }
  }

  @Test
  @Category({RunnableOnService.class, UsesTimersInParDo.class})
  public void testInBatchMode() {
    // TODO deal with infinite loop
    PCollection<String> collection =
        pipeline
            .apply("Input data", Create.of(data))
          // remove timestamps from dataset to be closer to users usecase
          .apply(
                "remove timestamps",
                ParDo.of(
                    new DoFn<TimestampedValue<KV<String, String>>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(c.element().getValue());
                      }
                    }))
            .apply(batchingParDo)
            .setCoder(StringUtf8Coder.of());
    PAssert.thatSingleton(collection.apply("Count", Count.<String>globally()))
        .isEqualTo(NUM_ELEMENTS);
    pipeline.run();
  }

  @Test
  @Category({RunnableOnService.class, UsesTimersInParDo.class, UsesTestStream.class})
  public void testInStreamingMode() {
    // TODO deal with infinite loop
    TestStream.Builder<KV<String, String>> streamBuilder =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(startInstant);
    for (TimestampedValue<KV<String, String>> element : data) {
      streamBuilder = streamBuilder.addElements(element);
    }
    TestStream<KV<String, String>> stream =
        streamBuilder
            //          .advanceProcessingTime(Duration.standardSeconds(10))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(5)))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(10)))
            //          .advanceProcessingTime(Duration.standardSeconds(20))
            .advanceWatermarkToInfinity();

    PCollection<String> output =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, String>>into(
                    FixedWindows.of(Duration.standardSeconds(WINDOW_DURATION))))
            .apply(batchingParDo)
            .setCoder(StringUtf8Coder.of());
    PAssert.that(output).containsInAnyOrder("Einstein2", "Darwin2");
    pipeline.run().waitUntilFinish();
  }

  private boolean checkAllElementsProcessing(List<String> output, Processing processing) {
    for (String element : output) {
      if (processing == Processing.PROCESSED) {
        if (!element.matches(".*2")) {
          return false;
        }
      } else {
        if (element.matches(".*2")) {
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
}
