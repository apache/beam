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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
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
  private static final int BATCH_SIZE = 100;
  private static final long NUM_ELEMENTS = 1000;
  private static final int TIMESTAMP_INTERVAL = 1;
  private static final int WINDOW_DURATION = 60;
  private static final long NUM_ELMENTS_PER_WINDOW = WINDOW_DURATION / TIMESTAMP_INTERVAL;
  private static final long NUM_WINDOWS = NUM_ELEMENTS / NUM_ELMENTS_PER_WINDOW;
  private static ArrayList<String> data;
  private static SimpleFunction<Iterable<String>, Iterable<String>> perBatchFn;
  private static BatchingParDo<String, String> batchingParDo;
  private static ArrayList<Long> expectedCollection = new ArrayList<>();
  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void initialize() {
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
    for (int i = 0; i < NUM_WINDOWS; i++) {
      expectedCollection.add(NUM_ELMENTS_PER_WINDOW);
    }
  }

  private static ArrayList<String> createTestData() {
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
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      int index = i % scientists.length;
      data.add(scientists[index]);
    }
    return data;
  }

  @Test
  public void testUnderlyingDoFn() throws Exception {
    DoFnTester<String, String> fnTester =
        DoFnTester.of(new BatchingParDo.BatchingDoFn<>(BATCH_SIZE, perBatchFn));
    int nbElementsProcessed = 0;
    for (String element : data) {
      fnTester.processElement(element);
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
  @Category(RunnableOnService.class)
  public void testInBatchMode() {
    PCollection<String> collection = pipeline.apply(Create.of(data)).apply(batchingParDo);
    PAssert.thatSingleton(collection.apply("Count", Count.<String>globally()))
        .isEqualTo(NUM_ELEMENTS);
    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testInStreamingMode() {

    PCollection<Long> collection = pipeline
      .apply("Input data", Create.of(data))
      .apply("Timestamp elements", ParDo.of(new AddTimestampFn()))
      // create windows to simulate unbounded source
      .apply(
        "Window elements in windows of 1 min",
        Window.<String>into(FixedWindows.of(Duration.standardSeconds(WINDOW_DURATION))))
      .apply("Batch process them", batchingParDo)
      .apply("Count per window", Count.<String>globally().withoutDefaults());
/*
    PAssert.that(collection)
        .satisfies(
            new SerializableFunction<Iterable<Long>, Void>() {
              @Override
              public Void apply(Iterable<Long> input) {
                ArrayList<Long> actualCollection = new ArrayList<Long>();
                for (Long element : input){
                  actualCollection.add(element);
                }
                assertEquals(
                    String.format(
                        "final collection must contain %s elements which value is %",
                        NUM_WINDOWS, NUM_ELMENTS_PER_WINDOW),
                    expectedCollection, actualCollection);
                return null;
              }
            });
*/
    pipeline.run();
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

  /** timestamp elements at 1 sec interval */
  static class AddTimestampFn extends DoFn<String, String> {
    private long millis = 0;

    @ProcessElement
    public void processElement(ProcessContext c) {
      millis += (TIMESTAMP_INTERVAL * 1000);
      c.outputWithTimestamp(c.element(), new Instant(millis));
    }
  }
}
